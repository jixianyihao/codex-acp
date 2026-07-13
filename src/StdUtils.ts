import {Readable, Transform, Writable} from "node:stream";
import {StringDecoder} from "node:string_decoder";
import {Emitter} from "vscode-jsonrpc/node";
import type {DataCallback, Disposable, Message, MessageReader, MessageWriter, PartialMessageInfo} from "vscode-jsonrpc/node";
import * as acp from "@agentclientprotocol/sdk";
import {logger} from "./Logger";
import {TraceRegistry, runWithTrace, traceRegistry, type TraceContext} from "./TraceContext";

export interface ProtocolMessageLogger {
    write(chunk: Uint8Array | string, encoding?: BufferEncoding): void;
    end(): void;
}

type ProtocolLogContext = object;

export function createProtocolMessageLogger(
    label: string,
    writeLog: (message: string, context?: ProtocolLogContext) => void = logger.log.bind(logger),
    resolveContext?: (message: unknown) => ProtocolLogContext | undefined,
): ProtocolMessageLogger {
    const decoder = new StringDecoder("utf8");
    let pending = "";
    let ended = false;

    const emitCompleteMessages = () => {
        const messages = pending.split("\n");
        pending = messages.pop() ?? "";
        for (const message of messages) {
            const normalized = message.endsWith("\r") ? message.slice(0, -1) : message;
            if (normalized.length > 0) {
                writeProtocolLog(normalized);
            }
        }
    };

    const writeProtocolLog = (message: string) => {
        let context: ProtocolLogContext | undefined;
        if (resolveContext) {
            try {
                context = resolveContext(JSON.parse(message));
            } catch {/* logging must not affect protocol handling */}
        }
        if (context) {
            writeLog(`[${label}] ${message}`, context);
        } else {
            writeLog(`[${label}] ${message}`);
        }
    };

    return {
        write(chunk, encoding) {
            if (ended) return;
            const bytes = typeof chunk === "string"
                ? Buffer.from(chunk, encoding ?? "utf8")
                : Buffer.from(chunk);
            pending += decoder.write(bytes);
            emitCompleteMessages();
        },
        end() {
            if (ended) return;
            ended = true;
            pending += decoder.end();
            const normalized = pending.endsWith("\r") ? pending.slice(0, -1) : pending;
            if (normalized.length > 0) {
                writeProtocolLog(normalized);
            }
            pending = "";
        },
    };
}

//TODO ask to include proper jsonrpc field and remove
export function createJSONRPCWriter(writable: Writable): MessageWriter {
    return {
        async write(msg: Message) {
            try {
                if (msg && typeof msg === 'object') {
                    // remove jsonrpc for the server
                    msg = {...msg};
                    delete (msg as any).jsonrpc;
                }
                writable.write(JSON.stringify(msg) + '\n');
            } catch {/* ignore */
            }
        },

        end() {
            writable.end();
        },
        onError: new Emitter<[Error, Message | undefined, number | undefined]>().event,
        onClose: new Emitter<void>().event,

        dispose() { }
    };
}

//TODO ask to include proper jsonrpc field and remove
export function createJSONRPCReader(
    readable: Readable,
    resolveContext?: (message: unknown) => TraceContext | undefined,
): MessageReader {
    return {
        listen(callback: DataCallback): Disposable {
            let buf = '';
            const onData = (chunk: Buffer) => {
                buf += chunk.toString();
                for (;;) {
                    const i = buf.indexOf('\n');
                    if (i < 0) break;
                    const line = buf.slice(0, i).trim();
                    buf = buf.slice(i + 1);
                    if (!line) continue;
                    try {
                        const msg = JSON.parse(line);
                        if (msg && typeof msg === 'object' && msg.jsonrpc === undefined) {
                            msg.jsonrpc = '2.0';
                        }
                        const context = resolveContext?.(msg);
                        if (context) {
                            runWithTrace(context, () => callback(msg));
                        } else {
                            callback(msg);
                        }
                    } catch {/* ignore malformed lines; they're still logged above */}
                }
            };
            readable.on('data', onData);
            return {
                dispose() {
                    readable.off('data', onData);
                }
            }
        },
        onError: new Emitter<Error>().event,
        onClose: new Emitter<void>().event,
        onPartialMessage: new Emitter<PartialMessageInfo>().event,
        dispose() {}
    }
}

export function createProtocolLoggingTransform(
    label: string,
    writeLog: (message: string, context?: ProtocolLogContext) => void = logger.log.bind(logger),
    resolveContext?: (message: unknown) => ProtocolLogContext | undefined,
): Transform {
    const messageLogger = createProtocolMessageLogger(label, writeLog, resolveContext);
    return new Transform({
        transform(chunk, encoding, callback) {
            messageLogger.write(chunk, encoding);
            callback(null, chunk);
        },
        flush(callback) {
            messageLogger.end();
            callback();
        },
    });
}

export function createJsonStream(readable: Readable, writable: Writable, registry: TraceRegistry = traceRegistry){
    const connectionContext = {connectionId: registry.connectionId};
    const incoming = createProtocolLoggingTransform("CLIENT->ACP", undefined, message => registry.resolveAcpIncoming(message) ?? connectionContext);
    const outgoing = createProtocolLoggingTransform("ACP->CLIENT", undefined, message => registry.resolveAcpOutgoing(message) ?? connectionContext);
    readable.pipe(incoming);
    outgoing.pipe(writable);

    const input = Writable.toWeb(outgoing);
    const output = Readable.toWeb(incoming) as ReadableStream<Uint8Array>;
    return acp.ndJsonStream(input, output);
}
