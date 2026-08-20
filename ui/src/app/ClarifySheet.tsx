// Presents one runtime-owned clarification without retaining its answer.
import type { FormEvent } from "react";
import { Button } from "../ui/Button";
import { Field } from "../ui/Field";
import { Sheet } from "../ui/Sheet";

export type ClarifyPrompt = {
  id: string;
  question: string;
  options: { label: string; recommended?: boolean }[];
};

type Props = {
  prompt: ClarifyPrompt;
  value: string;
  busy: boolean;
  onChange: (value: string) => void;
  onAnswer: (answer: string) => void;
  onCancel: () => void;
};

export function ClarifySheet({
  prompt,
  value,
  busy,
  onChange,
  onAnswer,
  onCancel,
}: Props) {
  const custom = value.trim();

  function submit(event: FormEvent) {
    event.preventDefault();
    if (custom && !busy) onAnswer(custom);
  }

  return (
    <Sheet id={prompt.id} title={prompt.question} onDismiss={onCancel}>
      {prompt.options.length > 0 && (
        <div className="clarify__options" aria-label="Suggested answers">
          {prompt.options.map((option) => (
            <Button
              key={option.label}
              tone={option.recommended ? "primary" : "quiet"}
              disabled={busy}
              onClick={() => onAnswer(option.label)}
            >
              <span>{option.label}</span>
              {option.recommended && <span className="clarify__recommended">Recommended</span>}
            </Button>
          ))}
        </div>
      )}
      <form className="clarify__custom" onSubmit={submit}>
        <Field
          id={`${prompt.id}-answer`}
          kind="textarea"
          label="Your answer"
          value={value}
          disabled={busy}
          onChange={(event) => onChange(event.currentTarget.value)}
        />
        <div className="clarify__actions">
          <Button tone="quiet" type="button" disabled={busy} onClick={onCancel}>
            Cancel
          </Button>
          <Button tone="primary" type="submit" disabled={!custom || busy}>
            Answer
          </Button>
        </div>
      </form>
    </Sheet>
  );
}
