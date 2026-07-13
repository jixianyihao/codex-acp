import * as acp from "@agentclientprotocol/sdk";
import {afterEach, describe, expect, it, vi} from "vitest";

import {
    createSpawnedAgentFixture,
    type SpawnedAgentFixture,
} from "./CodexACPAgent/e2e/spawned-agent-fixture";

describe("protocol logging integration", () => {
    let fixture: SpawnedAgentFixture | undefined;

    afterEach(async () => {
        await fixture?.dispose();
        vi.restoreAllMocks();
    });

    it("records a real ACP initialize round trip through Codex app-server", async () => {
        const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
        fixture = await createSpawnedAgentFixture(async connection => {
            await connection.initialize({
                protocolVersion: acp.PROTOCOL_VERSION,
                clientInfo: {
                    name: "protocol-logging-test",
                    version: "1.0.0",
                },
            });
        });

        await fixture.dispose();
        fixture = undefined;
        const logs = consoleLog.mock.calls.flat().join("\n");

        expect(logs).toMatch(/\[CLIENT->ACP\].*"method":"initialize"/);
        expect(logs).toMatch(/\[ACP->CLIENT\].*"result":/);
        expect(logs).toMatch(/\[ACP->APP_SERVER\].*"method":"initialize"/);
        expect(logs).toMatch(/\[APP_SERVER->ACP\].*"result":/);
        const traceId = logs.match(/\[CLIENT->ACP\].*"traceId":"([^"]+)"/)?.[1];
        expect(traceId).toBeDefined();
        const traceLines = logs.split("\n").filter(line => line.includes(`"traceId":"${traceId}"`));
        expect(traceLines.some(line => line.includes("[CLIENT->ACP]"))).toBe(true);
        expect(traceLines.some(line => line.includes("[ACP->APP_SERVER]"))).toBe(true);
        expect(traceLines.some(line => line.includes("[APP_SERVER->ACP]"))).toBe(true);
        expect(traceLines.some(line => line.includes("[ACP->CLIENT]"))).toBe(true);
        expect(traceLines.some(line => line.includes("[APP_SERVER->ACP]") && line.includes('"status":"ok"') && line.includes('"elapsedMs":'))).toBe(true);
        expect(traceLines.some(line => line.includes("[ACP->CLIENT]") && line.includes('"status":"ok"') && line.includes('"elapsedMs":'))).toBe(true);
        expect(logs).not.toContain("[TRACE ");
        expect(logs).not.toContain("[TIMING]");
        expect(logs).toContain("[APP_SERVER EXIT]");
    }, 20_000);
});
