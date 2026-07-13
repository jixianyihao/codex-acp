import * as rpc from "vscode-jsonrpc/node";
import type {MessageConnection} from "vscode-jsonrpc/node";
import type {ChildProcessWithoutNullStreams} from "node:child_process";
import {spawn} from "node:child_process";
import {createRequire} from "node:module";

import {createJSONRPCReader, createJSONRPCWriter, createProtocolMessageLogger} from "./StdUtils";
import {logger} from "./Logger";
import {TraceRegistry, traceRegistry} from "./TraceContext";

export interface CodexConnection {
    readonly connection: MessageConnection
    readonly process: ChildProcessWithoutNullStreams;
}

export function startCodexConnection(codexPath?: string, env?: NodeJS.ProcessEnv): CodexConnection {
    const spawnEnv = env ?? process.env;

    let codex: ChildProcessWithoutNullStreams;
    if (codexPath) {
        codex = process.platform === 'win32'
            ? spawn(`"${codexPath}" app-server`, { shell: true, env: spawnEnv })
            : spawn(codexPath, ['app-server'], { env: spawnEnv });
    } else {
        const bundledCodexPath = createRequire(import.meta.url).resolve("@openai/codex/bin/codex.js");
        codex = spawn(process.execPath, [bundledCodexPath, 'app-server'], {env: spawnEnv});
    }

    attachLogs(codex, traceRegistry);

    const reader = createJSONRPCReader(codex.stdout, message => traceRegistry.consumeAppServerIncoming(message));
    const writer = createJSONRPCWriter(codex.stdin);

    let connection = rpc.createMessageConnection(reader, writer);

    connection.listen();

    // Terminate all current activities on process termination
    codex.on("exit", _ => {
        connection.dispose();
    });

    return {connection: connection, process: codex};
}

export function attachLogs(proc: ChildProcessWithoutNullStreams, registry: TraceRegistry = traceRegistry) {
    const connectionContext = {connectionId: registry.connectionId};
    const stdinLogger = createProtocolMessageLogger("ACP->APP_SERVER", undefined, message => registry.resolveAppServerOutgoing(message) ?? connectionContext);
    const stdoutLogger = createProtocolMessageLogger("APP_SERVER->ACP", undefined, message => registry.peekAppServerIncoming(message) ?? connectionContext);
    const stderrLogger = createProtocolMessageLogger("APP_SERVER ERR", message => logger.log(message, connectionContext));
    const originalWrite = proc.stdin.write.bind(proc.stdin);
    proc.stdin.write = (chunk: any, encoding?: any, callback?: any): boolean => {
        stdinLogger.write(chunk, typeof encoding === "string" ? encoding as BufferEncoding : undefined);
        return originalWrite(chunk, encoding, callback);
    };
    proc.stdin.on("finish", () => stdinLogger.end());

    proc.stderr.on("data", (data) => {
        stderrLogger.write(data);
    });
    proc.stderr.on("end", () => stderrLogger.end());
    proc.stdout.on("data", (data: Buffer) => {
        stdoutLogger.write(data);
    });
    proc.stdout.on("end", () => stdoutLogger.end());
    proc.on("exit", (code) => {
        logger.log(`[APP_SERVER EXIT] code: ${code?.toString()}`, connectionContext);
        registry.clear();
    });
}
