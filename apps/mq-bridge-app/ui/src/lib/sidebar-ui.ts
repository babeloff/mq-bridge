export function registerDismissOnOutsideClick(
  isOpen: () => boolean,
  onDismiss: () => void,
  containerSelector = ".add-menu-container",
) {
  const handler = (event: MouseEvent) => {
    const target = event.target;
    const insideContainer = target instanceof HTMLElement && target.closest(containerSelector);
    if (isOpen() && !insideContainer) {
      onDismiss();
    }
  };

  let attached = false;
  // Defer attaching so the click that opened the menu finishes bubbling first,
  // otherwise it would reach this handler and immediately dismiss the menu.
  const timerId = setTimeout(() => {
    window.addEventListener("click", handler);
    attached = true;
  }, 0);

  return () => {
    clearTimeout(timerId);
    if (attached) {
      window.removeEventListener("click", handler);
    }
  };
}

export function startSidebarResize(
  event: MouseEvent,
  container: HTMLElement | null,
  onWidthChange: (width: number) => void,
) {
  event.preventDefault();
  const containerRect = container?.getBoundingClientRect();
  if (!containerRect) return;

  const minWidth = 220;
  const maxWidth = Math.max(minWidth, Math.min(640, Math.floor(containerRect.width * 0.6)));

  const onMove = (moveEvent: MouseEvent) => {
    const nextWidth = Math.min(maxWidth, Math.max(minWidth, moveEvent.clientX - containerRect.left));
    onWidthChange(nextWidth);
  };

  const onUp = () => {
    window.removeEventListener("mousemove", onMove);
    window.removeEventListener("mouseup", onUp);
  };

  window.addEventListener("mousemove", onMove);
  window.addEventListener("mouseup", onUp);
}
