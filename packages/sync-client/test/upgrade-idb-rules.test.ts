import { PROTOCOL_VERSION } from "@gateweave/track-protocol";
import { expect, it, vi } from "vitest";
import { DB_VERSION, openIdbPersistence, type Persistence, SyncClient, uuidv7 } from "../src";

function request<T>(req: IDBRequest<T>): Promise<T> {
	return new Promise((resolve, reject) => {
		req.onsuccess = () => resolve(req.result);
		req.onerror = () => reject(req.error);
	});
}

async function raw(key: string): Promise<IDBDatabase> {
	return request(indexedDB.open(`track:${key}`));
}

function change() {
	return {
		id: crypto.randomUUID(),
		createdAt: Date.now(),
		mutation: {
			name: "team.create" as const,
			args: { id: uuidv7(), key: "ENG", name: "Engineering" },
		},
	};
}

it("keeps ordered pending rows and client id through a storage version bump", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key);
	const clientId = (await old.load()).clientId;
	const first = change();
	const second = change();
	await old.addPending(first, second);
	old.close();
	// #331 had this row shape but no pendingFormat marker.
	const legacy = await raw(key);
	await request(
		legacy.transaction("meta", "readwrite").objectStore("meta").delete("pendingFormat"),
	);
	legacy.close();
	const next = await openIdbPersistence(key, { version: DB_VERSION + 1 });
	const stored = await next.load();
	expect(stored.clientId).toBe(clientId);
	expect(stored.pending).toEqual([first, second]);
	expect(stored.snapshot).toBeNull();
	next.close();
});

it("records the explicit pending format on a new database and preserves it on upgrade", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key);
	await old.load();
	old.close();
	const first = await raw(key);
	expect(await request(first.transaction("meta").objectStore("meta").get("pendingFormat"))).toBe(1);
	first.close();
	const next = await openIdbPersistence(key, { version: DB_VERSION + 1 });
	await next.load();
	next.close();
	const upgraded = await raw(key);
	expect(await request(upgraded.transaction("meta").objectStore("meta").get("pendingFormat"))).toBe(
		1,
	);
	upgraded.close();
});

it("aborts an unknown pending format without changing the old database or losing rows", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key);
	await old.load();
	const original = change();
	await old.addPending(original);
	old.close();
	const db = await raw(key);
	await request(db.transaction("meta", "readwrite").objectStore("meta").put(99, "pendingFormat"));
	db.close();
	await expect(openIdbPersistence(key, { version: DB_VERSION + 1 })).rejects.toThrow(
		"cannot read changes",
	);
	const unchanged = await raw(key);
	expect(unchanged.version).toBe(DB_VERSION);
	expect(
		(await request(unchanged.transaction("pending").objectStore("pending").getAll())).map(
			(row) => row.id,
		),
	).toEqual([original.id]);
	unchanged.close();
});

it("reports an unknown pending format through the client's refusal callback", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key);
	await old.load();
	old.close();
	const db = await raw(key);
	await request(db.transaction("meta", "readwrite").objectStore("meta").put(99, "pendingFormat"));
	db.close();
	const onReject = vi.fn();
	const client = new SyncClient({
		userId: "bob",
		persistence: openIdbPersistence(key, { version: DB_VERSION + 1 }),
		onReject,
		transport: {
			snapshot: async () => {
				throw new Error("Must not sync");
			},
			probe: async () => {},
			connect: () => ({ send() {}, close() {} }),
		},
	});
	await vi.waitFor(() => expect(onReject).toHaveBeenCalledOnce());
	expect(onReject.mock.calls[0]?.[0]).toMatchObject({
		storedUnreadable: true,
		mutation: { name: "Stored changes" },
	});
	expect(client.getStatus().connection).toBe("outdated");
	await client.close();
});

it("reports an unreadable stored row through onReject with its change name", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key);
	await old.load();
	const original = change();
	await old.addPending(original);
	old.close();
	const db = await raw(key);
	await request(
		db
			.transaction("pending", "readwrite")
			.objectStore("pending")
			.put({ ...original, seq: 1, mutation: { name: "retired.change", args: null } }),
	);
	db.close();
	const onReject = vi.fn();
	const client = new SyncClient({
		userId: "bob",
		persistence: openIdbPersistence(key),
		onReject,
		transport: {
			snapshot: async () => ({
				version: 0,
				teams: [],
				statuses: [],
				issues: [],
				labels: [],
				projects: [],
				projectUpdates: [],
				cycles: [],
				comments: [],
				activities: [],
				proposals: [],
				relations: [],
				views: [],
				templates: [],
				members: [],
				subscriptions: [],
				inbox: [],
				pullRequests: [],
			}),
			probe: async () => {},
			connect: () => ({ send() {}, close() {} }),
		},
	});
	await vi.waitFor(() => expect(onReject).toHaveBeenCalledOnce());
	expect(onReject.mock.calls[0]?.[0]).toMatchObject({
		storedUnreadable: true,
		mutation: { name: "retired.change" },
	});
	await client.close();
});

it("drains queued writes before an old tab closes on versionchange", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key);
	let releaseWrite = () => {};
	const writeGate = new Promise<void>((resolve) => {
		releaseWrite = resolve;
	});
	const persistence: Persistence = {
		shared: true,
		load: () => old.load(),
		replace: (snapshot) => old.replace(snapshot),
		applyOps: (ops, version) => old.applyOps(ops, version),
		addPending: async (...changes) => {
			await writeGate;
			await old.addPending(...changes);
		},
		removePending: (id) => old.removePending(id),
		close: () => old.close(),
		onTaken: (listener) =>
			old.onTaken?.(() => {
				listener();
				setTimeout(releaseWrite, 20);
			}),
	};
	const client = new SyncClient({
		userId: "bob",
		persistence,
		transport: {
			snapshot: async () => ({
				version: 0,
				teams: [],
				statuses: [],
				issues: [],
				labels: [],
				projects: [],
				projectUpdates: [],
				cycles: [],
				comments: [],
				activities: [],
				proposals: [],
				relations: [],
				views: [],
				templates: [],
				members: [],
				subscriptions: [],
				inbox: [],
				pullRequests: [],
			}),
			probe: async () => {},
			connect: () => ({ send() {}, close() {} }),
		},
	});
	await vi.waitFor(() => expect(client.getStatus().loaded).toBe(true));
	void client.mutate(change().mutation);
	const next = await openIdbPersistence(key, { version: DB_VERSION + 1 });
	expect((await next.load()).pending).toHaveLength(1);
	expect(client.getStatus().connection).toBe("outdated");
	expect(() => client.mutate(change().mutation)).toThrow("taken over");
	next.close();
	await client.close();
});

it("keeps #331's protocol upgrade behavior", async () => {
	const key = crypto.randomUUID();
	const old = await openIdbPersistence(key, { version: (PROTOCOL_VERSION - 1) * 1000 + 1 });
	await old.load();
	const original = change();
	await old.addPending(original);
	old.close();
	const next = await openIdbPersistence(key);
	expect((await next.load()).pending).toEqual([original]);
	next.close();
});
