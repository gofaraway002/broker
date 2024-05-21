import {
  CONFIGURATION,
  findProjectRoot,
  getConfig,
  loadBrokerConfig,
} from '../../common/config/config';
import { LoadedClientOpts } from '../../common/types/options';
import { log as logger } from '../../logs/logger';
import { retrieveConnectionsForDeployment } from '../config/remoteConfig';
import { IdentifyingMetadata, WebSocketConnection } from '../types/client';

export const setConnectionWatcher = (
  clientOpts,
  websocketConnections: WebSocketConnection[],
  connectionIdentifyingMetadata: IdentifyingMetadata,
  callBack: (
    websocketConnections: WebSocketConnection[],
    clientOpts: LoadedClientOpts,
    identifyingMetadata: IdentifyingMetadata,
  ) => void,
) => {
  let connectionWatcherId;

  const connectionWatcher = async () => {
    try {
      logger.debug(
        {
          connectionName: connectionIdentifyingMetadata.friendlyName,
          connectionId: connectionIdentifyingMetadata.id,
        },
        'Connection Watcher checking for connection usage.',
      );

      await retrieveConnectionsForDeployment(
        clientOpts,
        `${findProjectRoot(__dirname)}/config.universal.json`,
      );
      // Reload config with connection
      await loadBrokerConfig();
      const globalConfig = { config: getConfig() };
      clientOpts.config = Object.assign(
        {},
        clientOpts.config,
        globalConfig.config,
      ) as Record<string, any> as CONFIGURATION;

      clearTimeout(connectionWatcherId);

      if (
        clientOpts.config.connections[
          `${connectionIdentifyingMetadata.friendlyName}`
        ].identifier
      ) {
        logger.info(
          {
            connectionName: connectionIdentifyingMetadata.friendlyName,
            connectionId: connectionIdentifyingMetadata.id,
          },
          'Connection is now in use. Establishing tunnel to server.',
        );
        connectionIdentifyingMetadata.identifier =
          clientOpts.config.connections[
            `${connectionIdentifyingMetadata.friendlyName}`
          ].identifier;
        callBack(
          websocketConnections,
          clientOpts,
          connectionIdentifyingMetadata,
        );
      } else {
        connectionWatcherId = setTimeout(
          connectionWatcher,
          clientOpts.config.connectionsManager.watcher.interval,
        );
      }
    } catch (err) {
      logger.error(
        {
          connectionName: connectionIdentifyingMetadata.friendlyName,
          connectionId: connectionIdentifyingMetadata.id,
          err,
        },
        `Connection Watcher error checking for connection usage.`,
      );
      throw new Error(`${err}`);
    }
  };
  connectionWatcherId = setTimeout(
    connectionWatcher,
    clientOpts.config.connectionsManager.watcher.frequency,
  );
};
