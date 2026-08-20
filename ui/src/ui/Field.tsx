// Owns the shared multiline text control.
import { forwardRef, type TextareaHTMLAttributes } from "react";

type Props = TextareaHTMLAttributes<HTMLTextAreaElement> & {
  label: string;
};

export const Field = forwardRef<HTMLTextAreaElement, Props>(function Field(
  { label, id, ...props },
  ref,
) {
  return (
    <label className="field" htmlFor={id}>
      <span className="field__label">{label}</span>
      <textarea ref={ref} className="field__input" id={id} {...props} />
    </label>
  );
});
