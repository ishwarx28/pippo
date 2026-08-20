// Owns the shared modal surface and its keyboard boundary.
import { useEffect, useRef, type KeyboardEvent, type ReactNode } from "react";

type Props = {
  id: string;
  title: string;
  children: ReactNode;
  onDismiss: () => void;
};

const focusable =
  'button:not(:disabled), input:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])';

export function Sheet({ id, title, children, onDismiss }: Props) {
  const panel = useRef<HTMLElement>(null);

  useEffect(() => {
    const prior = document.activeElement instanceof HTMLElement ? document.activeElement : undefined;
    const first = panel.current?.querySelector<HTMLElement>(focusable) ?? panel.current;
    first?.focus();
    return () => prior?.focus();
  }, []);

  function keyDown(event: KeyboardEvent<HTMLElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      onDismiss();
      return;
    }
    if (event.key !== "Tab" || !panel.current) return;
    const controls = [...panel.current.querySelectorAll<HTMLElement>(focusable)];
    if (controls.length === 0) {
      event.preventDefault();
      panel.current.focus();
      return;
    }
    const first = controls[0];
    const last = controls[controls.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  return (
    <div className="sheet-scrim">
      <section
        ref={panel}
        className="sheet"
        role="dialog"
        aria-modal="true"
        aria-labelledby={`${id}-title`}
        tabIndex={-1}
        onKeyDown={keyDown}
      >
        <h2 className="sheet__title" id={`${id}-title`}>
          {title}
        </h2>
        {children}
      </section>
    </div>
  );
}
