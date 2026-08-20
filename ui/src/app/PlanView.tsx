// Owns the plan proposal shown in the chat, with Proceed and hand editing.
import { useState } from "react";
import { Button } from "../ui/Button";
import { Card } from "../ui/Card";
import { Field } from "../ui/Field";

export type StepStatus = "todo" | "running" | "done" | "failed" | "skipped";

export type Step = {
  id: string;
  title: string;
  detail: string;
  files: string[];
  verify: string;
  risk: string;
  status: StepStatus;
  note?: string;
};

export type Plan = {
  task_id: string;
  project_id: string;
  run_id: string;
  goal: string;
  path: string;
  proceeded: boolean;
  text: string;
  steps: Step[];
};

type Props = {
  plan: Plan;
  busy: boolean;
  onProceed: () => void;
  onSave: (text: string) => void;
};

export function PlanView({ plan, busy, onProceed, onSave }: Props) {
  const [draft, setDraft] = useState<string>();

  function save() {
    if (draft === undefined || busy) return;
    onSave(draft);
    setDraft(undefined);
  }

  return (
    <Card
      title={plan.goal}
      footer={
        plan.proceeded ? (
          <p className="plan__state">Working through the plan.</p>
        ) : draft === undefined ? (
          <>
            <Button tone="primary" disabled={busy} onClick={onProceed}>
              Proceed
            </Button>
            <Button tone="quiet" disabled={busy} onClick={() => setDraft(plan.text)}>
              Edit
            </Button>
          </>
        ) : (
          <>
            <Button tone="primary" disabled={busy} onClick={save}>
              Save plan
            </Button>
            <Button tone="quiet" disabled={busy} onClick={() => setDraft(undefined)}>
              Cancel
            </Button>
          </>
        )
      }
    >
      {draft === undefined ? (
        <ol className="plan__steps">
          {plan.steps.map((step) => (
            <li className={`plan__step plan__step--${step.status}`} key={step.id}>
              <p className="plan__step-title">
                <span className="plan__dot" aria-hidden="true" />
                {step.title}
                <span className="plan__status">{step.status}</span>
              </p>
              {step.detail && <p className="plan__detail">{step.detail}</p>}
              <dl className="plan__meta">
                {step.files.length > 0 && (
                  <>
                    <dt>Files</dt>
                    <dd>{step.files.join(", ")}</dd>
                  </>
                )}
                <dt>Verify</dt>
                <dd>{step.verify}</dd>
                <dt>Risk</dt>
                <dd>{step.risk}</dd>
                {step.note && (
                  <>
                    <dt>Note</dt>
                    <dd>{step.note}</dd>
                  </>
                )}
              </dl>
            </li>
          ))}
        </ol>
      ) : (
        <Field
          id="plan-text"
          label="Plan"
          labelHidden
          rows={16}
          value={draft}
          spellCheck={false}
          disabled={busy}
          onChange={(event) => setDraft(event.currentTarget.value)}
        />
      )}
    </Card>
  );
}
