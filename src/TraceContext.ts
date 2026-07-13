import {AsyncLocalStorage} from "node:async_hooks";
import {randomUUID} from "node:crypto";

export type TraceRequestId = string | number | null;

export interface TraceContext {
    connectionId: string;
    traceId: string;
    spanId: string;
    parentSpanId: string | null;
    acpRequestId?: TraceRequestId;
    appServerRequestId?: TraceRequestId;
    method?: string;
    sessionId?: string;
    threadId?: string;
    turnId?: string;
}

export interface TraceLogContext extends TraceContext {
    status?: "ok" | "error";
    elapsedMs?: number;
}

const storage = new AsyncLocalStorage<TraceContext>();
let childSpanSequence = 0;

function requestKey(id: TraceRequestId): string {
    return `${typeof id}:${String(id)}`;
}

export class TraceRegistry {
    readonly connectionId: string;
    private readonly acpRequests = new Map<string, TraceContext>();
    private readonly acpClientRequests = new Map<string, TraceContext>();
    private readonly appServerRequests = new Map<string, TraceContext>();
    private readonly appServerClientRequests = new Map<string, TraceContext>();
    private readonly acpRequestStartedAt = new Map<string, number>();
    private readonly acpClientRequestStartedAt = new Map<string, number>();
    private readonly appServerRequestStartedAt = new Map<string, number>();
    private readonly appServerClientRequestStartedAt = new Map<string, number>();
    private readonly relatedIds = new Map<string, TraceContext>();

    constructor(connectionId: string = randomUUID()) {
        this.connectionId = connectionId;
    }

    getOrCreateAcpRequest(method: string, requestId: TraceRequestId): TraceContext {
        const key = requestKey(requestId);
        const existing = this.acpRequests.get(key);
        if (existing) return existing;

        const traceId = `${this.connectionId}:${key}`;
        const context: TraceContext = {
            connectionId: this.connectionId,
            traceId,
            spanId: `${traceId}:root`,
            parentSpanId: null,
            acpRequestId: requestId,
            method,
        };
        this.acpRequests.set(key, context);
        this.acpRequestStartedAt.set(key, performance.now());
        return context;
    }

    resolveAcpIncoming(message: unknown): TraceLogContext | undefined {
        const record = asRecord(message);
        if (!record) return undefined;
        const requestId = getRequestId(record);
        const method = typeof record["method"] === "string" ? record["method"] : undefined;

        if (method && requestId !== undefined) {
            const context = this.withIdentifiers(this.getOrCreateAcpRequest(method, requestId), record);
            this.acpRequests.set(requestKey(requestId), context);
            this.rememberIdentifiers(context);
            return context;
        }
        if (requestId !== undefined) {
            const key = requestKey(requestId);
            const context = this.acpClientRequests.get(key);
            const startedAt = this.acpClientRequestStartedAt.get(key);
            this.acpClientRequests.delete(key);
            this.acpClientRequestStartedAt.delete(key);
            return context ? this.withCompletion(this.withIdentifiers(context, record), record, startedAt) : undefined;
        }
        return this.contextForMessage(record);
    }

    resolveAcpOutgoing(message: unknown): TraceLogContext | undefined {
        const record = asRecord(message);
        if (!record) return undefined;
        const requestId = getRequestId(record);
        const method = typeof record["method"] === "string" ? record["method"] : undefined;

        if (requestId !== undefined && !method) {
            const key = requestKey(requestId);
            const context = this.acpRequests.get(key) ?? this.contextForMessage(record) ?? currentTraceContext();
            const startedAt = this.acpRequestStartedAt.get(key);
            this.acpRequests.delete(key);
            this.acpRequestStartedAt.delete(key);
            if (!context) return undefined;
            const enriched = this.withIdentifiers(context, record);
            this.rememberIdentifiers(enriched);
            return this.withCompletion(enriched, record, startedAt);
        }

        const context = currentTraceContext() ?? this.contextForMessage(record);
        if (!context) return undefined;
        const enriched = this.withIdentifiers(context, record);
        this.rememberIdentifiers(enriched);
        if (requestId !== undefined && method) {
            const key = requestKey(requestId);
            this.acpClientRequests.set(key, enriched);
            this.acpClientRequestStartedAt.set(key, performance.now());
        }
        return enriched;
    }

    resolveAppServerOutgoing(message: unknown): TraceLogContext | undefined {
        const record = asRecord(message);
        if (!record) return undefined;
        const requestId = getRequestId(record);
        const method = typeof record["method"] === "string" ? record["method"] : undefined;

        if (requestId !== undefined && !method) {
            const key = requestKey(requestId);
            const context = this.appServerClientRequests.get(key) ?? currentTraceContext() ?? this.contextForMessage(record);
            const startedAt = this.appServerClientRequestStartedAt.get(key);
            this.appServerClientRequests.delete(key);
            this.appServerClientRequestStartedAt.delete(key);
            return context ? this.withCompletion(this.withAppServerMessage(context, record, requestId), record, startedAt) : undefined;
        }

        const context = currentTraceContext() ?? this.contextForMessage(record);
        if (!context) return undefined;
        const enriched = this.withAppServerMessage(context, record, requestId, method);
        if (requestId !== undefined && method) {
            const key = requestKey(requestId);
            this.appServerRequests.set(key, enriched);
            this.appServerRequestStartedAt.set(key, performance.now());
        }
        return enriched;
    }

    peekAppServerIncoming(message: unknown): TraceLogContext | undefined {
        return this.resolveAppServerIncoming(message, true);
    }

    private resolveAppServerIncoming(message: unknown, includeCompletion: boolean): TraceLogContext | undefined {
        const record = asRecord(message);
        if (!record) return undefined;
        const requestId = getRequestId(record);
        const method = typeof record["method"] === "string" ? record["method"] : undefined;
        const key = requestId !== undefined ? requestKey(requestId) : undefined;
        const context = key && !method
            ? this.appServerRequests.get(key) ?? this.contextForMessage(record)
            : this.contextForMessage(record) ?? currentTraceContext();
        if (!context) return undefined;
        const enriched = this.withAppServerMessage(context, record, requestId, method);
        return includeCompletion && key && !method
            ? this.withCompletion(enriched, record, this.appServerRequestStartedAt.get(key))
            : enriched;
    }

    consumeAppServerIncoming(message: unknown): TraceContext | undefined {
        const record = asRecord(message);
        if (!record) return undefined;
        const requestId = getRequestId(record);
        const method = typeof record["method"] === "string" ? record["method"] : undefined;
        const context = this.resolveAppServerIncoming(record, false);

        if (requestId !== undefined && method && context) {
            const key = requestKey(requestId);
            this.appServerClientRequests.set(key, context);
            this.appServerClientRequestStartedAt.set(key, performance.now());
        } else if (requestId !== undefined && !method) {
            const key = requestKey(requestId);
            this.appServerRequests.delete(key);
            this.appServerRequestStartedAt.delete(key);
        }
        return context;
    }

    contextForMessage(message: unknown): TraceContext | undefined {
        const identifiers = extractIdentifiers(message);
        return identifiers.turnId && this.relatedIds.get(identifiers.turnId)
            || identifiers.threadId && this.relatedIds.get(identifiers.threadId)
            || identifiers.sessionId && this.relatedIds.get(identifiers.sessionId)
            || undefined;
    }

    clear(): void {
        this.acpRequests.clear();
        this.acpClientRequests.clear();
        this.appServerRequests.clear();
        this.appServerClientRequests.clear();
        this.acpRequestStartedAt.clear();
        this.acpClientRequestStartedAt.clear();
        this.appServerRequestStartedAt.clear();
        this.appServerClientRequestStartedAt.clear();
        this.relatedIds.clear();
    }

    private withIdentifiers(context: TraceContext, message: unknown): TraceContext {
        return {...context, ...extractIdentifiers(message)};
    }

    private rememberIdentifiers(context: TraceContext): void {
        for (const id of [context.sessionId, context.threadId, context.turnId]) {
            if (id) this.relatedIds.set(id, context);
        }
    }

    private withAppServerMessage(
        context: TraceContext,
        message: unknown,
        requestId: TraceRequestId | undefined,
        method?: string,
    ): TraceContext {
        let enriched = this.withIdentifiers(context, message);
        if (requestId !== undefined) enriched = {...enriched, appServerRequestId: requestId};
        if (method) enriched = {...enriched, method};
        this.rememberIdentifiers(enriched);
        return enriched;
    }

    private withCompletion(context: TraceContext, message: Record<string, unknown>, startedAt: number | undefined): TraceLogContext {
        if (startedAt === undefined) return context;
        return {
            ...context,
            status: "error" in message ? "error" : "ok",
            elapsedMs: performance.now() - startedAt,
        };
    }
}

export const traceRegistry = new TraceRegistry();

export function currentTraceContext(): TraceContext | undefined {
    return storage.getStore();
}

export function runWithTrace<T>(context: TraceContext, callback: () => T): T {
    return storage.run(context, callback);
}

export function createChildTrace(method: string): TraceContext | undefined {
    const parent = currentTraceContext();
    if (!parent) return undefined;
    const {appServerRequestId: _appServerRequestId, ...inherited} = parent;

    return {
        ...inherited,
        spanId: `${parent.traceId}:span-${++childSpanSequence}`,
        parentSpanId: parent.spanId,
        method,
    };
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
    return typeof value === "object" && value !== null ? value as Record<string, unknown> : undefined;
}

function getRequestId(message: Record<string, unknown>): TraceRequestId | undefined {
    if (!("id" in message)) return undefined;
    const id = message["id"];
    return typeof id === "string" || typeof id === "number" || id === null ? id : undefined;
}

function extractIdentifiers(value: unknown): Partial<Pick<TraceContext, "sessionId" | "threadId" | "turnId">> {
    const identifiers: Partial<Pick<TraceContext, "sessionId" | "threadId" | "turnId">> = {};

    const visit = (candidate: unknown, parentKey: string | undefined, depth: number): void => {
        if (depth > 5) return;
        if (Array.isArray(candidate)) {
            for (const item of candidate) visit(item, parentKey, depth + 1);
            return;
        }
        const record = asRecord(candidate);
        if (!record) return;

        for (const [key, nested] of Object.entries(record)) {
            if ((key === "sessionId" || key === "threadId" || key === "turnId") && typeof nested === "string") {
                identifiers[key] ??= nested;
            } else if (key === "id" && typeof nested === "string") {
                if (parentKey === "session") identifiers.sessionId ??= nested;
                if (parentKey === "thread") identifiers.threadId ??= nested;
                if (parentKey === "turn") identifiers.turnId ??= nested;
            }
            visit(nested, key, depth + 1);
        }
    };

    visit(value, undefined, 0);
    return identifiers;
}
