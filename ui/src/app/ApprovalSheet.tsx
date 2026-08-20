// Presents one runtime-owned approval on the shared sheet surface.
import { Button } from "../ui/Button";
import { Sheet } from "../ui/Sheet";

export type ApprovalPrompt = {
  id: string;
  rule_id: string;
  tool: "shell" | "write" | "edit";
  subject: string;
  path: string;
  reason: string;
};

type Choice = "allow_once" | "allow_session" | "deny";

type Props = {
  prompt: ApprovalPrompt;
  busy: boolean;
  onChoose: (choice: Choice) => void;
  onCancel: () => void;
};

export function ApprovalSheet({ prompt, busy, onChoose, onCancel }: Props) {
  return (
    <Sheet id={prompt.id} title={`Approve ${prompt.tool}?`} onDismiss={onCancel}>
      <dl className="approval__details">
        <dt>Action</dt>
        <dd>{prompt.tool}</dd>
        <dt>{prompt.tool === "shell" ? "Command" : "Path"}</dt>
        <dd><code>{prompt.subject}</code></dd>
        {prompt.tool === "shell" && (
          <>
            <dt>Working directory</dt>
            <dd><code>{prompt.path}</code></dd>
          </>
        )}
        <dt>Rule</dt>
        <dd><code>{prompt.rule_id}</code></dd>
        <dt>Reason</dt>
        <dd>{prompt.reason}</dd>
      </dl>
      <div className="approval__actions">
        <Button tone="danger" disabled={busy} onClick={() => onChoose("deny")}>
          Deny
        </Button>
        <Button tone="quiet" disabled={busy} onClick={() => onChoose("allow_once")}>
          Allow once
        </Button>
        <Button tone="primary" disabled={busy} onClick={() => onChoose("allow_session")}>
          Allow for session
        </Button>
      </div>
    </Sheet>
  );
}
