// Owns the event-backed conversation projection and command wiring.
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { useEffect, useReducer, useRef, useState } from "react";
import { Composer } from "./Composer";
import { ClarifySheet, type ClarifyPrompt } from "./ClarifySheet";
import { Header } from "./Header";
import { MessageView } from "./Message";
import { ModelKeyCard } from "./ModelKeyCard";
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
type KeyStatus = "missing" | "stored";
type MessageAction = TurnEvent | { kind: "hydrate"; messages: Message[] };
type InteractionEvent =
  | { kind: "clarify_opened"; prompt: ClarifyPrompt }
  | { kind: "clarify_closed"; id: string; error?: string };

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
  const [keyDraft, setKeyDraft] = useState("");
  const [keyStatus, setKeyStatus] = useState<KeyStatus>();
  const [keySaving, setKeySaving] = useState(false);
  const [keyError, setKeyError] = useState<string>();
  const [sending, setSending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [clarify, setClarify] = useState<ClarifyPrompt>();
  const [clarifyDraft, setClarifyDraft] = useState("");
  const [clarifyBusy, setClarifyBusy] = useState(false);
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
    let active = true;
    let stop: (() => void) | undefined;
    async function watchInteractions() {
      try {
        const unlisten = await listen<InteractionEvent>("interaction-event", ({ payload }) => {
          if (!active) return;
          if (payload.kind === "clarify_opened") {
            setClarify(payload.prompt);
            setClarifyDraft("");
            setClarifyBusy(false);
            setAnnouncement("Question needs your answer");
          } else {
            setClarify((current) => (current?.id === payload.id ? undefined : current));
            setClarifyDraft("");
            setClarifyBusy(false);
            if (payload.error) {
              setError(payload.error);
              setAnnouncement(payload.error);
            }
          }
        });
        if (!active) {
          unlisten();
          return;
        }
        stop = unlisten;
      } catch (reason) {
        if (active) setError(`Could not receive questions: ${detail(reason)}`);
      }
    }
    void watchInteractions();
    return () => {
      active = false;
      stop?.();
    };
  }, []);

  useEffect(() => {
    let active = true;
    let stop: (() => void) | undefined;
    async function checkKey() {
      try {
        const unlisten = await listen<KeyStatus>("model-key-status", ({ payload }) => {
          if (!active) return;
          setKeyDraft("");
          setKeyStatus(payload);
          setKeyError(undefined);
          setAnnouncement(payload === "stored" ? "Model key stored" : "Model key required");
        });
        if (!active) {
          unlisten();
          return;
        }
        stop = unlisten;
        const status = await invoke<KeyStatus>("model_key_status");
        if (active) setKeyStatus(status);
      } catch (reason) {
        if (active) setError(`Could not check model access: ${detail(reason)}`);
      }
    }
    void checkKey();
    return () => {
      active = false;
      stop?.();
    };
  }, []);

  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" });
  }, [keyStatus, messages]);

  async function send() {
    const text = draft.trim();
    if (!text || keyStatus !== "stored" || running || sending) return;
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

  async function storeKey() {
    const value = keyDraft.trim();
    if (!value || keySaving) return;
    setKeySaving(true);
    setKeyError(undefined);
    setKeyDraft("");
    try {
      const status = await invoke<KeyStatus>("store_model_key", { value });
      if (status === "stored") setAnnouncement("Model key stored");
    } catch {
      setKeyError("Could not store the model key. Try again.");
    } finally {
      setKeySaving(false);
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

  async function answerClarify(answer: string) {
    if (!clarify || clarifyBusy) return;
    setClarifyBusy(true);
    setError(undefined);
    try {
      await invoke("answer_clarify", { id: clarify.id, answer });
      setAnnouncement("Answer sent");
    } catch (reason) {
      setClarifyBusy(false);
      setError(`Could not answer the question: ${detail(reason)}`);
    }
  }

  async function cancelClarify() {
    if (!clarify || clarifyBusy) return;
    setClarifyBusy(true);
    setError(undefined);
    try {
      await invoke("cancel_clarify", { id: clarify.id });
    } catch (reason) {
      setClarifyBusy(false);
      setError(`Could not cancel the question: ${detail(reason)}`);
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
        {messages.map((message) => <MessageView key={message.id} message={message} />)}
        {keyStatus === "missing" && (
          <ModelKeyCard
            value={keyDraft}
            saving={keySaving}
            error={keyError}
            onChange={setKeyDraft}
            onSubmit={() => void storeKey()}
          />
        )}
        {messages.length === 0 && keyStatus === "stored" && (
          <p className="transcript__empty">What shall we work on?</p>
        )}
        {messages.length === 0 && keyStatus === undefined && (
          <p className="transcript__empty" role="status">
            Checking model access…
          </p>
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
        enabled={keyStatus === "stored"}
        running={running}
        sending={sending}
        stopping={stopping}
        onChange={setDraft}
        onSend={() => void send()}
        onStop={() => void stop()}
      />
      {clarify && (
        <ClarifySheet
          prompt={clarify}
          value={clarifyDraft}
          busy={clarifyBusy}
          onChange={setClarifyDraft}
          onAnswer={(answer) => void answerClarify(answer)}
          onCancel={() => void cancelClarify()}
        />
      )}
      <p className="sr-only" aria-live="polite" aria-atomic="true">
        {announcement}
      </p>
    </main>
  );
}
