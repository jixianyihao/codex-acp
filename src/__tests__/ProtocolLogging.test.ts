import {EventEmitter} from "node:events";
import {PassThrough, Readable, Writable} from "node:stream";
import {pipeline} from "node:stream/promises";
import {afterEach, describe, expect, it, vi} from "vitest";
import type {AnyMessage} from "@agentclientprotocol/sdk";
import type {ChildProcessWithoutNullStreams} from "node:child_process";
import type {MessageConnection} from "vscode-jsonrpc/node";

import {CodexAppServerClient} from "../CodexAppServerClient";
import {attachLogs} from "../CodexJsonRpcConnection";
import {logger} from "../Logger";
import {createJSONRPCReader, createJsonStream, createProtocolLoggingTransform} from "../StdUtils";
import {TraceRegistry, createChildTrace, currentTraceContext, runWithTrace} from "../TraceContext";
import {createSmartMock} from "./acp-test-utils";

describe("protocol logging", () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("logs ACP bytes without changing them", async () => {
        const payload = Buffer.from('{"jsonrpc":"2.0","id":1,"method":"session/new"}\n');
        const output: Buffer[] = [];
        const writeLog = vi.fn();
        const loggingTransform = createProtocolLoggingTransform("CLIENT->ACP", writeLog);

        await pipeline(
            Readable.from([payload]),
            loggingTransform,
            new Writable({
                write(chunk, _encoding, callback) {
                    output.push(Buffer.from(chunk));
                    callback();
                },
            }),
        );

        expect(Buffer.concat(output)).toEqual(payload);
        expect(writeLog).toHaveBeenCalledWith(`[CLIENT->ACP] ${payload.toString().trimEnd()}`);
    });

    it("logs one complete entry per NDJSON message across split and combined chunks", async () => {
        const output: Buffer[] = [];
        const writeLog = vi.fn();
        const loggingTransform = createProtocolLoggingTransform("CLIENT->ACP", writeLog);

        await pipeline(
            Readable.from([
                Buffer.from('{"id":1,"method":"first"}\n{"id":2,"meth'),
                Buffer.from('od":"second"}\n'),
            ]),
            loggingTransform,
            new Writable({
                write(chunk, _encoding, callback) {
                    output.push(Buffer.from(chunk));
                    callback();
                },
            }),
        );

        expect(Buffer.concat(output).toString()).toBe('{"id":1,"method":"first"}\n{"id":2,"method":"second"}\n');
        expect(writeLog.mock.calls).toEqual([
            ['[CLIENT->ACP] {"id":1,"method":"first"}'],
            ['[CLIENT->ACP] {"id":2,"method":"second"}'],
        ]);
    });

    it("logs ACP requests, responses, and notifications in both directions", async () => {
        const input = new PassThrough();
        const output = new PassThrough();
        const outputChunks: Buffer[] = [];
        output.on("data", chunk => outputChunks.push(Buffer.from(chunk)));
        const log = vi.spyOn(logger, "log").mockImplementation(() => undefined);
        const stream = createJsonStream(input, output);
        const reader = stream.readable.getReader();
        const writer = stream.writable.getWriter();
        const messages: AnyMessage[] = [
            {jsonrpc: "2.0", id: 1, method: "session/new", params: {cwd: "C:/workspace", mcpServers: []}},
            {jsonrpc: "2.0", id: 1, result: {sessionId: "thread-1"}},
            {jsonrpc: "2.0", method: "session/update", params: {sessionId: "thread-1", update: {sessionUpdate: "agent_message_chunk", content: {type: "text", text: "ok"}}}},
        ];

        for (const message of messages) {
            input.write(`${JSON.stringify(message)}\n`);
            expect((await reader.read()).value).toEqual(message);
            await writer.write(message);
        }

        await vi.waitFor(() => {
            expect(Buffer.concat(outputChunks).toString()).toBe(messages.map(message => `${JSON.stringify(message)}\n`).join(""));
        });
        for (const message of messages) {
            const payload = `${JSON.stringify(message)}\n`;
            expect(log.mock.calls.some(([line]) => line === `[CLIENT->ACP] ${payload.trimEnd()}`)).toBe(true);
            expect(log.mock.calls.some(([line]) => line === `[ACP->CLIENT] ${payload.trimEnd()}`)).toBe(true);
        }

        input.end();
        await writer.close();
        reader.releaseLock();
    });

    it("correlates concurrent ACP requests with their responses", async () => {
        const input = new PassThrough();
        const output = new PassThrough();
        output.resume();
        const registry = new TraceRegistry("connection-test");
        const log = vi.spyOn(logger, "log").mockImplementation(() => undefined);
        const stream = createJsonStream(input, output, registry);
        const reader = stream.readable.getReader();
        const writer = stream.writable.getWriter();
        const firstRequest = {jsonrpc: "2.0", id: 1, method: "initialize", params: {protocolVersion: 1}};
        const secondRequest = {jsonrpc: "2.0", id: 2, method: "session/new", params: {cwd: "C:/workspace", mcpServers: []}};

        input.write(`${JSON.stringify(firstRequest)}\n${JSON.stringify(secondRequest)}\n`);
        await reader.read();
        await reader.read();
        await writer.write({jsonrpc: "2.0", id: 2, result: {sessionId: "thread-2"}});
        await writer.write({jsonrpc: "2.0", id: 1, result: {agentCapabilities: {}}});

        const firstInput = log.mock.calls.find(([line]) => line === `[CLIENT->ACP] ${JSON.stringify(firstRequest)}`)?.[1];
        const secondInput = log.mock.calls.find(([line]) => line === `[CLIENT->ACP] ${JSON.stringify(secondRequest)}`)?.[1];
        const firstOutput = log.mock.calls.find(([line]) => line.startsWith(`[ACP->CLIENT] {"jsonrpc":"2.0","id":1,`))?.[1];
        const secondOutput = log.mock.calls.find(([line]) => line.startsWith(`[ACP->CLIENT] {"jsonrpc":"2.0","id":2,`))?.[1];

        expect(firstInput).toMatchObject({connectionId: "connection-test", acpRequestId: 1, method: "initialize"});
        expect(secondInput).toMatchObject({connectionId: "connection-test", acpRequestId: 2, method: "session/new"});
        expect(firstInput).not.toHaveProperty("elapsedMs");
        expect(secondInput).not.toHaveProperty("elapsedMs");
        expect(firstOutput).toMatchObject({traceId: (firstInput as {traceId: string}).traceId, status: "ok", elapsedMs: expect.any(Number)});
        expect(secondOutput).toMatchObject({traceId: (secondInput as {traceId: string}).traceId, sessionId: "thread-2", status: "ok", elapsedMs: expect.any(Number)});
        expect((firstInput as {traceId: string}).traceId).not.toBe((secondInput as {traceId: string}).traceId);

        input.end();
        await writer.close();
        reader.releaseLock();
    });

    it("logs app-server requests, responses, notifications, stderr, and exit", async () => {
        const stdin = new PassThrough();
        const stdout = new PassThrough();
        const stderr = new PassThrough();
        const process = Object.assign(new EventEmitter(), {stdin, stdout, stderr}) as unknown as ChildProcessWithoutNullStreams;
        const log = vi.spyOn(logger, "log").mockImplementation(() => undefined);
        const payloads = [
            '{"id":1,"method":"model/list","params":{}}\n',
            '{"id":1,"result":{"data":[]}}\n',
            '{"method":"thread/started","params":{"threadId":"thread-1"}}\n',
        ];

        attachLogs(process);
        stdin.write(payloads.join(""));
        stdout.write(payloads[0]?.slice(0, 12));
        stdout.write(`${payloads[0]?.slice(12)}${payloads[1]}${payloads[2]}`);
        stderr.write("first diagnostic\nsecond diagnostic\n");
        process.emit("exit", 0);

        for (const payload of payloads) {
            expect(log.mock.calls.some(([line]) => line === `[ACP->APP_SERVER] ${payload.trimEnd()}`)).toBe(true);
            expect(log.mock.calls.some(([line]) => line === `[APP_SERVER->ACP] ${payload.trimEnd()}`)).toBe(true);
        }
        expect(log).toHaveBeenCalledWith("[APP_SERVER ERR] first diagnostic", expect.objectContaining({connectionId: expect.any(String)}));
        expect(log).toHaveBeenCalledWith("[APP_SERVER ERR] second diagnostic", expect.objectContaining({connectionId: expect.any(String)}));
        expect(log).toHaveBeenCalledWith("[APP_SERVER EXIT] code: 0", expect.objectContaining({connectionId: expect.any(String)}));
    });

    it("correlates an app-server request, response, and notification", () => {
        const stdin = new PassThrough();
        const stdout = new PassThrough();
        const stderr = new PassThrough();
        const process = Object.assign(new EventEmitter(), {stdin, stdout, stderr}) as unknown as ChildProcessWithoutNullStreams;
        const registry = new TraceRegistry("connection-test");
        const root = registry.getOrCreateAcpRequest("session/prompt", 7);
        const child = runWithTrace(root, () => createChildTrace("turn/start"));
        expect(child).toBeDefined();
        const log = vi.spyOn(logger, "log").mockImplementation(() => undefined);
        const callbackContexts: unknown[] = [];

        attachLogs(process, registry);
        const reader = createJSONRPCReader(stdout, message => registry.consumeAppServerIncoming(message));
        const disposable = reader.listen(() => callbackContexts.push(currentTraceContext()));
        runWithTrace(child!, () => stdin.write('{"id":10,"method":"turn/start","params":{"threadId":"thread-1"}}\n'));
        stdout.write('{"id":10,"result":{"turn":{"id":"turn-1"}}}\n');
        stdout.write('{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1"}}}\n');

        const requestContext = log.mock.calls.find(([line]) => line.startsWith("[ACP->APP_SERVER]"))?.[1];
        const responseContext = log.mock.calls.find(([line]) => line.startsWith("[APP_SERVER->ACP] {\"id\":10"))?.[1];
        const notificationContext = log.mock.calls.find(([line]) => line.startsWith("[APP_SERVER->ACP] {\"method\":\"turn/completed\""))?.[1];
        expect(requestContext).toMatchObject({traceId: root.traceId, spanId: child!.spanId, parentSpanId: root.spanId, appServerRequestId: 10});
        expect(responseContext).toMatchObject({traceId: root.traceId, spanId: child!.spanId, appServerRequestId: 10, turnId: "turn-1", status: "ok", elapsedMs: expect.any(Number)});
        expect(notificationContext).toMatchObject({traceId: root.traceId, turnId: "turn-1", threadId: "thread-1"});
        expect(notificationContext).not.toHaveProperty("elapsedMs");
        expect(callbackContexts).toHaveLength(2);
        expect(callbackContexts[0]).toMatchObject({traceId: root.traceId, appServerRequestId: 10});
        expect(callbackContexts[1]).toMatchObject({traceId: root.traceId, turnId: "turn-1"});

        disposable.dispose();
    });

    it("runs app-server requests in a child trace without separate timing logs", async () => {
        let requestContext: ReturnType<typeof currentTraceContext>;
        const connection = createSmartMock<MessageConnection>(() => undefined, {
            returnValues: new Map([
                ["sendRequest", () => {
                    requestContext = currentTraceContext();
                    return Promise.resolve({data: [], nextCursor: null});
                }],
            ]),
        });
        const log = vi.spyOn(logger, "log").mockImplementation(() => undefined);
        const client = new CodexAppServerClient(connection);
        const registry = new TraceRegistry("connection-test");
        const root = registry.getOrCreateAcpRequest("session/new", 1);

        await runWithTrace(root, () => client.listModels({cursor: null, limit: null}));

        expect(requestContext).toMatchObject({traceId: root.traceId, parentSpanId: root.spanId, method: "model/list"});
        expect(log.mock.calls.some(([message]) => String(message).includes("[TIMING]"))).toBe(false);
    });

    it("preserves app-server request failures without separate timing logs", async () => {
        const error = new Error("request failed");
        const connection = createSmartMock<MessageConnection>(() => undefined, {
            returnValues: new Map([
                ["sendRequest", () => Promise.reject(error)],
            ]),
        });
        const log = vi.spyOn(logger, "log").mockImplementation(() => undefined);
        const client = new CodexAppServerClient(connection);

        await expect(client.listModels({cursor: null, limit: null})).rejects.toBe(error);
        expect(log.mock.calls.some(([message]) => String(message).includes("[TIMING]"))).toBe(false);
    });
});
