// Owns the shared structured content surface.
import type { ReactNode } from "react";

type Props = {
  title: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  tone?: "wait" | "bad";
};

export function Card({ title, children, footer, tone }: Props) {
  return (
    <article className={`card${tone ? ` card--${tone}` : ""}`}>
      <header className="card__header">
        <h2 className="card__title">{title}</h2>
      </header>
      <div className="card__body">{children}</div>
      {footer && <footer className="card__footer">{footer}</footer>}
    </article>
  );
}
