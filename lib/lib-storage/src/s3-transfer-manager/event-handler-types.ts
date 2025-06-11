/**
 * Function type for handling transfer events in the transfer manager.
 * Represents a callback that receives event data during transfer operations.
 * 
 * @param event - The event object containing transfer details and progress information.

 * @public
 */
export type EventHandlerFunction<E = Event> = (event: Event & E) => void;

/**
 * Union type for handling transfer events in the transfer manager.
 * Can be a function or an object.
 *
 * @public
 */
export type EventHandler<E = Event> = EventHandlerFunction<E> | EventHandlerObject<E>;

/**
 * Object type for handling transfer events in the transfer manager.
 * Represents an object that implements the `handleEvent` method to handle transfer events.
 *
 * @public
 */
export type EventHandlerObject<E = Event> = {
  handleEvent: EventHandlerFunction<E>;
};

/**
 * Configuration options for registering event listeners in the transfer manager.
 * Controls the behavior of event listeners for transfer events.
 *
 * @public
 */
export type AddEventListenerOptions = {
  once?: boolean;
  passive?: boolean;
  signal?: AbortSignal;
  capture?: boolean;
};

/**
 * Configuration options for removing event listeners in the transfer manager.
 * Controls the behavior of event listeners for transfer events.
 *
 * @public
 */
export type RemoveEventListenerOptions = {
  capture?: boolean;
};
