// Lets the leader key ask for "open the Nth sidebar thread" without owning the
// sidebar's thread order, which only the mounted sidebar knows.
const THREAD_JUMP_EVENT = "t3code:thread-jump";

export function requestThreadJump(index: number): void {
  window.dispatchEvent(new CustomEvent(THREAD_JUMP_EVENT, { detail: { index } }));
}

export function onThreadJumpRequest(listener: (index: number) => void): () => void {
  const handler = (event: Event) => {
    listener((event as CustomEvent<{ index: number }>).detail.index);
  };
  window.addEventListener(THREAD_JUMP_EVENT, handler);
  return () => window.removeEventListener(THREAD_JUMP_EVENT, handler);
}
