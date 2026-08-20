// Owns the shared labelled text controls and their errors.
import {
  forwardRef,
  type InputHTMLAttributes,
  type TextareaHTMLAttributes,
} from "react";

type Shared = {
  id: string;
  label: string;
  labelHidden?: boolean;
  error?: string;
};

type Props = Shared &
  (
    | ({ kind: "input" } & InputHTMLAttributes<HTMLInputElement>)
    | ({ kind?: "textarea" } & TextareaHTMLAttributes<HTMLTextAreaElement>)
  );

export const Field = forwardRef<HTMLInputElement | HTMLTextAreaElement, Props>(function Field(
  { label, labelHidden = false, error, id, ...props },
  ref,
) {
  const describedBy = error ? `${id}-error` : undefined;
  let control;
  if (props.kind === "input") {
    const { kind: _kind, ...input } = props;
    control = (
      <input
        {...input}
        ref={ref as React.ForwardedRef<HTMLInputElement>}
        className="field__input"
        id={id}
        aria-invalid={Boolean(error)}
        aria-describedby={describedBy}
      />
    );
  } else {
    const { kind: _kind, ...textarea } = props;
    control = (
      <textarea
        {...textarea}
        ref={ref as React.ForwardedRef<HTMLTextAreaElement>}
        className="field__input field__input--multiline"
        id={id}
        aria-invalid={Boolean(error)}
        aria-describedby={describedBy}
      />
    );
  }
  return (
    <label className="field" htmlFor={id}>
      <span className={labelHidden ? "field__label field__label--hidden" : "field__label"}>
        {label}
      </span>
      {control}
      {error && (
        <span className="field__error" id={describedBy} role="alert">
          {error}
        </span>
      )}
    </label>
  );
});
