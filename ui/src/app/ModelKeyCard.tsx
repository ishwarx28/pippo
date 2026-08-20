// Owns model credential entry shown inside the conversation.
import type { FormEvent } from "react";
import { Button } from "../ui/Button";
import { Card } from "../ui/Card";
import { Field } from "../ui/Field";

type Props = {
  value: string;
  saving: boolean;
  error?: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
};

export function ModelKeyCard({ value, saving, error, onChange, onSubmit }: Props) {
  function submit(event: FormEvent) {
    event.preventDefault();
    if (value.trim() && !saving) onSubmit();
  }

  return (
    <Card title="Connect a model" tone="wait">
      <p className="model-key-card__copy">
        Add your Gemini API key to start chatting. It is stored only in the system keychain.
      </p>
      <form className="model-key-card__form" onSubmit={submit}>
        <Field
          kind="input"
          id="model-key"
          label="Gemini API key"
          type="password"
          value={value}
          error={error}
          placeholder="Enter API key"
          autoComplete="off"
          spellCheck={false}
          disabled={saving}
          autoFocus
          onChange={(event) => onChange(event.currentTarget.value)}
        />
        <Button type="submit" tone="primary" disabled={saving || !value.trim()}>
          {saving ? "Saving…" : "Save key"}
        </Button>
      </form>
    </Card>
  );
}
