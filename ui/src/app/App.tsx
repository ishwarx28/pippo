// Owns the event-backed conversation projection and command wiring.
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { useEffect, useReducer, useRef, useState } from "react";
import { Composer } from "./Composer";
import { Header } from "./Header";
import { MessageView } from "./Message";
import "../ui/ui.css";
import "./app.css";

type Status = "running" | "done" | "failed" | "cancelled";
type Role = "user" | "assistant";

export type Message = {
  id: string;
  turn_id: string;
  role: Role;
  text: string;
  status?: Status;
  error?: string;
};

type Opened = {
  kind: "opened";
  turn_id: string;
  request_id: string;
  user: Message;
  assistant: Message;
};

type Chunk = {
  kind: "chunk";
  turn_id: string;
  request_id: string;
  message_id: string;
  text: string;
};

type Closed = {
  kind: "closed";
  turn_id: string;
  request_id: string;
  message_id: string;
  status: Exclude<Status, "running">;
  error?: string;
};

type TurnEvent = Opened | Chunk | Closed;
type MessageAction = TurnEvent | { kind: "hydrate"; messages: Message[] };

function applyEvent(messages: Message[], event: MessageAction): Message[] {
  if (event.kind === "hydrate") {
    const live = new Map(messages.map((message) => [message.id, message]));
    const stored = new Set(event.messages.map((message) => message.id));
    return [
      ...event.messages.map((message) => live.get(message.id) ?? message),
      ...messages.filter((message) => !stored.has(message.id)),
    ];
  }
  if (event.kind === "opened") {
    const ids = new Set([event.user.id, event.assistant.id]);
    return [...messages.filter((message) => !ids.has(message.id)), event.user, event.assistant];
  }
  return messages.map((message) => {
    if (message.id !== event.message_id || message.turn_id !== event.turn_id) return message;
    if (event.kind === "chunk" && message.status === "running") {
      return { ...message, text: message.text + event.text };
    }
    if (event.kind === "closed" && message.status === "running") {
      return { ...message, status: event.status, error: event.error };
    }
    return message;
  });
}

function detail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function App() {
  const [messages, dispatch] = useReducer(applyEvent, []);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [error, setError] = useState<string>();
  const [announcement, setAnnouncement] = useState("");
  const opened = useRef(0);
  const end = useRef<HTMLDivElement>(null);
  const running = messages.some(
    (message) => message.role === "assistant" && message.status === "running",
  );

  useEffect(() => {
    let active = true;
    let stop: (() => void) | undefined;
    async function hydrate() {
      try {
        const unlisten = await listen<TurnEvent>("turn-event", ({ payload }) => {
          if (!active) return;
          dispatch(payload);
          if (payload.kind === "opened") {
            opened.current += 1;
            setDraft("");
            setError(undefined);
            setAnnouncement("Turn started");
          } else if (payload.kind === "closed") {
            setStopping(false);
            setAnnouncement(
              payload.status === "done"
                ? "Reply complete"
                : payload.status === "cancelled"
                  ? "Turn stopped"
                  : "Turn failed",
            );
          }
        });
        if (!active) {
          unlisten();
          return;
        }
        stop = unlisten;
        const messages = await invoke<Message[]>("session_snapshot");
        if (active) dispatch({ kind: "hydrate", messages });
      } catch (reason) {
        if (active) setError(`Could not restore the conversation: ${detail(reason)}`);
      }
    }
    void hydrate();
    return () => {
      active = false;
      stop?.();
    };
  }, []);

  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" });
  }, [messages]);

  async function send() {
    const text = draft.trim();
    if (!text || running || sending) return;
    const count = opened.current;
    setSending(true);
    setError(undefined);
    try {
      await invoke("send_message", { text });
    } catch (reason) {
      if (opened.current === count) setError(`Could not send message: ${detail(reason)}`);
    } finally {
      setSending(false);
    }
  }

  async function stop() {
    if (!running || stopping) return;
    setStopping(true);
    setError(undefined);
    try {
      const cancelled = await invoke<boolean>("stop_turn");
      if (!cancelled) setStopping(false);
    } catch (reason) {
      setStopping(false);
      setError(`Could not stop the turn: ${detail(reason)}`);
    }
  }

  return (
    <main className="shell">
      <Header running={running} />
      <section
        className="transcript"
        aria-label="Conversation"
        aria-live="polite"
        aria-busy={running}
      >
        {messages.length === 0 ? (
          <p className="transcript__empty">What shall we work on?</p>
        ) : (
          messages.map((message) => <MessageView key={message.id} message={message} />)
        )}
        <div ref={end} />
      </section>
      {error && (
        <p className="notice" role="alert">
          {error}
        </p>
      )}
      <Composer
        value={draft}
        running={running}
        sending={sending}
        stopping={stopping}
        onChange={setDraft}
        onSend={() => void send()}
        onStop={() => void stop()}
      />
      <p className="sr-only" aria-live="polite" aria-atomic="true">
        {announcement}
      </p>
    </main>
  );
}
