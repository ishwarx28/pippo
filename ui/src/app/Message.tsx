// Owns one user or assistant transcript entry.
import type { Message } from "./App";

type Props = {
  message: Message;
};

export function MessageView({ message }: Props) {
  const pending = message.role === "assistant" && message.status === "running";
  return (
    <article className={`message message--${message.role}`}>
      {message.text}
      {pending && !message.text && (
        <span className="message__pending" role="status" aria-label="Reply pending" />
      )}
      {message.status === "cancelled" && (
        <p className="message__state">Stopped.</p>
      )}
      {message.status === "failed" && (
        <p className="message__state message__error" role="alert">
          {message.error || "The reply failed."}
        </p>
      )}
    </article>
  );
}
