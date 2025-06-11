export type EventHandlerFunction<E = Event> = (event: Event & E) => void;

export type EventHandlerObject<E = Event> = {
  handleEvent: EventHandlerFunction<E>;
};

export type AddEventListenerOptions = {
  once?: boolean;
  passive?: boolean;
  signal?: AbortSignal;
  capture?: boolean;
};

export type RemoveEventListenerOptions = {
  capture?: boolean;
};

export type EventHandler<E = Event> = EventHandlerFunction<E> | EventHandlerObject<E>;
