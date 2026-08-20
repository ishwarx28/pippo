// Owns message entry and the current turn action.
import { useEffect, useRef, type KeyboardEvent } from "react";
import { Button } from "../ui/Button";
import { Field } from "../ui/Field";

type Props = {
  value: string;
  enabled: boolean;
  running: boolean;
  sending: boolean;
  stopping: boolean;
  onChange: (value: string) => void;
  onSend: () => void;
  onStop: () => void;
};

export function Composer({
  value,
  enabled,
  running,
  sending,
  stopping,
  onChange,
  onSend,
  onStop,
}: Props) {
  const input = useRef<HTMLTextAreaElement>(null);
  const stop = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (running) stop.current?.focus();
    else if (enabled) input.current?.focus();
  }, [enabled, running]);

  function keyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing) return;
    event.preventDefault();
    if (value.trim() && enabled && !running && !sending) onSend();
  }

  return (
    <form
      className="composer"
      onSubmit={(event) => {
        event.preventDefault();
        onSend();
      }}
    >
      <Field
        ref={input}
        id="message"
        label="Message"
        labelHidden
        value={value}
        placeholder={
          running ? "Reply in progress" : enabled ? "Message pippo" : "Add a model key to continue"
        }
        disabled={running || !enabled}
        onChange={(event) => onChange(event.currentTarget.value)}
        onKeyDown={keyDown}
      />
      {running ? (
        <Button ref={stop} type="button" tone="danger" disabled={stopping} onClick={onStop}>
          {stopping ? "Stopping…" : "Stop"}
        </Button>
      ) : (
        <Button type="submit" tone="primary" disabled={!enabled || sending || !value.trim()}>
          {sending ? "Sending…" : "Send"}
        </Button>
      )}
    </form>
  );
}
