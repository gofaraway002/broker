import { LoadedClientOpts } from '../../common/types/options';
import { IdentifyingMetadata, WebSocketConnection } from '../types/client';
import { translateIntegrationTypeToBrokerIntegrationType } from '../utils/integrations';
import { log as logger } from '../../logs/logger';
import { createWebSocketConnectionPairs } from '../socket';
import { setConnectionWatcher } from './watcher';
import { getServerId } from '../dispatcher';
import { retrieveConnectionsForDeployment } from '../config/remoteConfig';
import { findProjectRoot } from '../../common/config/config';
import { handleTerminationSignal } from '../../common/utils/signals';
import { cleanUpUniversalFile } from '../utils/cleanup';
import { validateUniversalConnectionsRemoteConfig } from './validator';
import { existsSync, writeFileSync } from 'fs';
import { fetchJwt } from '../auth/oauth';
import { reloadConfig } from '../config/configHelpers';

export const manageWebsocketConnections = async (
  clientOpts: LoadedClientOpts,
  globalIdentifyingMetadata: IdentifyingMetadata,
): Promise<WebSocketConnection[]> => {
  try {
    if (!process.env.SKIP_REMOTE_CONFIG) {
      if (!existsSync(`${findProjectRoot(__dirname)}/config.universal.json`)) {
        process.env.NO_UNIVERSAL_FILE = 'true';
        writeFileSync(
          `${findProjectRoot(__dirname)}/config.universal.json`,
          JSON.stringify({
            BROKER_CLIENT_CONFIGURATION: {
              common: {
                oauth: {
                  clientId: '${CLIENT_ID}',
                  clientSecret: '${CLIENT_SECRET}',
                },
              },
            },
          }),
        );
        await reloadConfig(clientOpts);
      }

      if (!process.env.SKIP_REMOTE_CONFIG) {
        clientOpts.accessToken = await fetchJwt(
          clientOpts.config.API_BASE_URL,
          clientOpts.config.brokerClientConfiguration.common.oauth.clientId,
          clientOpts.config.brokerClientConfiguration.common.oauth.clientSecret,
        );

        await retrieveConnectionsForDeployment(
          clientOpts,
          `${findProjectRoot(__dirname)}/config.universal.json`,
        );

        validateUniversalConnectionsRemoteConfig(
          `${findProjectRoot(__dirname)}/config.universal.json`,
        );
      }
      handleTerminationSignal(cleanUpUniversalFile);
    }

    await reloadConfig(clientOpts);
  } catch (err) {
    logger.error({ err }, 'Error loading connections from remote');
  }
  const integrationsKeys = clientOpts.config.connections
    ? Object.keys(clientOpts.config.connections)
    : [];
  const websocketConnections: WebSocketConnection[] = [];
  for (let i = 0; i < integrationsKeys.length; i++) {
    const socketIdentifyingMetadata = structuredClone(
      globalIdentifyingMetadata,
    );
    socketIdentifyingMetadata.friendlyName = integrationsKeys[i];
    socketIdentifyingMetadata.id =
      clientOpts.config.connections[`${integrationsKeys[i]}`].id;
    socketIdentifyingMetadata.identifier =
      clientOpts.config.connections[`${integrationsKeys[i]}`]['identifier'];
    socketIdentifyingMetadata.isDisabled =
      clientOpts.config.connections[`${integrationsKeys[i]}`].isDisabled ??
      false;
    const integrationType =
      clientOpts.config.connections[`${integrationsKeys[i]}`].type;

    // This type is different for broker, ECR/ACR/DockeHub/etc are all container-registry-agent type for broker
    socketIdentifyingMetadata.supportedIntegrationType =
      translateIntegrationTypeToBrokerIntegrationType(integrationType);
    socketIdentifyingMetadata.serverId =
      clientOpts.config.connections[`${integrationsKeys[i]}`].serverId ?? '';
    if (socketIdentifyingMetadata.isDisabled) {
      logger.warn(
        {
          id: socketIdentifyingMetadata.id,
          name: socketIdentifyingMetadata.friendlyName,
        },
        `Connection is disabled due to (a) missing environment variable(s). Please provide the value and restart the broker client.`,
      );
    } else if (!socketIdentifyingMetadata.identifier) {
      logger.warn(
        {
          id: socketIdentifyingMetadata.id,
          name: socketIdentifyingMetadata.friendlyName,
        },
        `Connection not in use by any orgs. Will check periodically and create connection when in use.`,
      );
      setConnectionWatcher(
        clientOpts,
        websocketConnections,
        socketIdentifyingMetadata,
        createWebSocketConnectionPairs,
      );
    } else {
      let serverId: string | null = null;
      if (clientOpts.config.BROKER_HA_MODE_ENABLED == 'true') {
        serverId = await getServerId(
          clientOpts.config,
          clientOpts.config.connections[`${integrationsKeys[i]}`].identifier,
          socketIdentifyingMetadata.clientId,
        );
      }
      if (serverId === null) {
        logger.warn({}, 'could not receive server id from Broker Dispatcher');
        serverId = '';
      } else {
        logger.info({ serverId }, 'received server id');
        clientOpts.config.connections[`${integrationsKeys[i]}`].serverId =
          serverId;
      }
      createWebSocketConnectionPairs(
        websocketConnections,
        clientOpts,
        socketIdentifyingMetadata,
      );
    }
  }
  return websocketConnections;
};
