// Owns the minimal session header.
type Props = {
  running: boolean;
};

export function Header({ running }: Props) {
  return (
    <header className="header">
      <h1 className="header__wordmark">pippo</h1>
      <span className="header__status" role="status">
        <span
          className={`status-dot${running ? " status-dot--running" : ""}`}
          aria-hidden="true"
        />
        {running ? "Live" : "Idle"}
      </span>
    </header>
  );
}
