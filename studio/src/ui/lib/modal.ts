export interface ModalOptions {
  onclose: () => void;
  initialFocus?: () => HTMLElement | null | undefined;
}

export function modal(
  dialog: HTMLDialogElement,
  options: ModalOptions,
): { update: (next: ModalOptions) => void; destroy: () => void } {
  let current = options;
  const previous =
    document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
  const root = document.documentElement;
  root.dataset.modal = String(Number(root.dataset.modal ?? 0) + 1);

  const onCancel = (event: Event) => {
    event.preventDefault();
  };
  const onClose = () => {
    current.onclose();
  };
  dialog.addEventListener("cancel", onCancel);
  dialog.addEventListener("close", onClose);

  if (!dialog.open) dialog.showModal();
  const target = current.initialFocus?.();
  if (target) target.focus({ preventScroll: true });

  return {
    update(next) {
      current = next;
    },
    destroy() {
      dialog.removeEventListener("cancel", onCancel);
      dialog.removeEventListener("close", onClose);
      const depth = Number(root.dataset.modal ?? 1) - 1;
      if (depth > 0) root.dataset.modal = String(depth);
      else delete root.dataset.modal;
      if (previous?.isConnected) previous.focus({ preventScroll: true });
    },
  };
}
