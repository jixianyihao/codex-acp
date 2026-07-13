import fs from "node:fs";
import path from "node:path";
import {afterEach, describe, expect, it} from "vitest";

import {Logger} from "../Logger";
import {
    TraceRegistry,
    createChildTrace,
    currentTraceContext,
    runWithTrace,
} from "../TraceContext";

describe("trace context", () => {
    const originalLogDir = process.env["APP_SERVER_LOGS"];
    const createdLogDirs: string[] = [];

    afterEach(() => {
        if (originalLogDir === undefined) {
            delete process.env["APP_SERVER_LOGS"];
        } else {
            process.env["APP_SERVER_LOGS"] = originalLogDir;
        }
        for (const logDir of createdLogDirs.splice(0)) {
            fs.rmSync(logDir, {recursive: true, force: true});
        }
    });

    it("isolates concurrent ACP requests and creates child spans", async () => {
        const registry = new TraceRegistry("connection-test");
        const first = registry.getOrCreateAcpRequest("initialize", 1);
        const second = registry.getOrCreateAcpRequest("session/new", 2);

        expect(first.traceId).not.toBe(second.traceId);

        const [firstCurrent, secondCurrent] = await Promise.all([
            runWithTrace(first, async () => {
                await new Promise(resolve => setImmediate(resolve));
                const child = createChildTrace("model/list");
                expect(child).toMatchObject({
                    connectionId: first.connectionId,
                    traceId: first.traceId,
                    parentSpanId: first.spanId,
                    acpRequestId: 1,
                    method: "model/list",
                });
                return currentTraceContext();
            }),
            runWithTrace(second, async () => {
                await new Promise(resolve => setImmediate(resolve));
                return currentTraceContext();
            }),
        ]);

        expect(firstCurrent).toBe(first);
        expect(secondCurrent).toBe(second);
    });

    it("adds active trace fields to logger context while preserving explicit fields", () => {
        const logDir = fs.mkdtempSync(path.join(process.cwd(), "trace-logs-"));
        createdLogDirs.push(logDir);
        process.env["APP_SERVER_LOGS"] = logDir;
        const instance = new Logger();
        const registry = new TraceRegistry("connection-test");
        const root = registry.getOrCreateAcpRequest("session/new", "request-1");

        runWithTrace(root, () => instance.log("trace logger", {method: "explicit-method", marker: true}));

        const line = fs.readFileSync(path.join(logDir, "app-server.log"), "utf8").trim();
        const context = JSON.parse(line.slice(line.indexOf("{")));
        expect(context).toMatchObject({
            connectionId: "connection-test",
            traceId: root.traceId,
            spanId: root.spanId,
            parentSpanId: null,
            acpRequestId: "request-1",
            method: "explicit-method",
            marker: true,
        });
    });

    it("correlates reverse ACP and app-server requests", () => {
        const registry = new TraceRegistry("connection-test");
        const root = registry.getOrCreateAcpRequest("session/prompt", 1);
        const child = runWithTrace(root, () => createChildTrace("turn/start"));

        const appRequest = runWithTrace(child!, () => registry.resolveAppServerOutgoing({
            id: 10,
            method: "turn/start",
            params: {threadId: "thread-1"},
        }));
        registry.consumeAppServerIncoming({id: 10, result: {turn: {id: "turn-1"}}});
        const appReverseRequest = registry.consumeAppServerIncoming({
            id: 11,
            method: "item/commandExecution/requestApproval",
            params: {threadId: "thread-1", turnId: "turn-1"},
        });
        const appReverseResponse = registry.resolveAppServerOutgoing({id: 11, result: {decision: "accept"}});
        const acpReverseRequest = runWithTrace(appReverseRequest!, () => registry.resolveAcpOutgoing({
            jsonrpc: "2.0",
            id: 20,
            method: "session/request_permission",
            params: {sessionId: "thread-1"},
        }));
        const acpReverseResponse = registry.resolveAcpIncoming({jsonrpc: "2.0", id: 20, result: {outcome: "cancelled"}});

        for (const context of [appRequest, appReverseRequest, appReverseResponse, acpReverseRequest, acpReverseResponse]) {
            expect(context).toMatchObject({traceId: root.traceId});
        }
        expect(appRequest).toMatchObject({appServerRequestId: 10});
        expect(appReverseRequest).toMatchObject({appServerRequestId: 11, threadId: "thread-1", turnId: "turn-1"});
        expect(appReverseResponse).toMatchObject({appServerRequestId: 11});
    });

    it("marks error responses with elapsed time", () => {
        const registry = new TraceRegistry("connection-test");
        registry.resolveAcpIncoming({jsonrpc: "2.0", id: 1, method: "session/new", params: {}});
        const acpError = registry.resolveAcpOutgoing({
            jsonrpc: "2.0",
            id: 1,
            error: {code: -32603, message: "failed"},
        });

        const root = registry.getOrCreateAcpRequest("session/prompt", 2);
        const child = runWithTrace(root, () => createChildTrace("turn/start"));
        runWithTrace(child!, () => registry.resolveAppServerOutgoing({id: 10, method: "turn/start", params: {}}));
        const appServerError = registry.peekAppServerIncoming({id: 10, error: {code: -32603, message: "failed"}});

        expect(acpError).toMatchObject({status: "error", elapsedMs: expect.any(Number)});
        expect(appServerError).toMatchObject({status: "error", elapsedMs: expect.any(Number)});
    });
});
