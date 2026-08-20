// Owns the shared text action control.
import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from "react";

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  tone: "primary" | "quiet" | "danger";
  children: ReactNode;
};

export const Button = forwardRef<HTMLButtonElement, Props>(function Button(
  { tone, children, className = "", ...props },
  ref,
) {
  return (
    <button ref={ref} className={`button button--${tone} ${className}`.trim()} {...props}>
      {children}
    </button>
  );
});
