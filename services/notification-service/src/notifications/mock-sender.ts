import type { Logger } from '../lib/logger.js';
import type { NotificationSender } from './types.js';

/**
 * Mock delivery: nothing is actually sent, we just log what would have been.
 * Swap this for an SES/Twilio/FCM-backed sender in a real deployment.
 */
export function createMockSender(logger: Logger): NotificationSender {
  return {
    async send(notification) {
      logger.info(
        {
          notificationId: notification.id,
          userId: notification.userId,
          channel: notification.channel,
          template: notification.template,
        },
        `would send ${notification.channel.toLowerCase()}`,
      );
    },
  };
}
