import {gql, useQuery} from '@apollo/client';
import * as React from 'react';

import {AssetLink} from './assets/AssetLink';
import {Box} from './ui/Box';
import {Group} from './ui/Group';
import {MainContent} from './ui/MainContent';

export const PoopRoot = () => {
  const {data, loading} = useQuery(POOP_GRAPH_QUERY);
  if (loading) {
    return 'LOADING';
  }

  const {repositoriesOrError} = data;

  if (repositoriesOrError.__typename !== 'RepositoryConnection') {
    return 'ERROR';
  }

  console.log(repositoriesOrError);

  return (
    <MainContent>
      <Group spacing={24} direction="column">
        {repositoriesOrError.nodes.map((repository) => (
          <Box key={repository.id}>
            <Group spacing={12} direction="column">
              {repository.pipelines.map((pipeline) => (
                <Box key={pipeline.id}>
                  <div>{pipeline.name}</div>
                  <div>
                    {repository.name}@{repository.location.name}
                  </div>
                  <div>Produced Assets</div>
                  <Box margin={{left: 24}}>
                    <div>Declared</div>
                    <Group spacing={4} direction="column" margin={{horizontal: 24, vertical: 12}}>
                      {pipeline.declaredAssetKeys.length ? (
                        pipeline.declaredAssetKeys.map((assetKey) => (
                          <AssetLink key={assetKey.id} assetKey={assetKey} />
                        ))
                      ) : (
                        <span>None</span>
                      )}
                    </Group>
                    <div>Historical</div>
                    <Group spacing={4} direction="column" margin={{horizontal: 24, vertical: 12}}>
                      {pipeline.historicalAssetKeys.length ? (
                        pipeline.historicalAssetKeys.map((assetKey) => (
                          <AssetLink key={assetKey.id} assetKey={assetKey} />
                        ))
                      ) : (
                        <span>None</span>
                      )}
                    </Group>
                  </Box>
                  <div>Upstream Assets</div>
                  <Group spacing={0} direction="column" margin={{left: 24}}>
                    {pipeline.sensors.filter((sensor) => sensor.isAssetSensor).length ? (
                      pipeline.sensors
                        .filter((sensor) => sensor.isAssetSensor)
                        .map((sensor) => (
                          <Group key={sensor.id} spacing={8} direction="column">
                            {sensor.assets.map((asset) => (
                              <>
                                <AssetLink key={asset.key.id} assetKey={asset.key} />
                                [via {sensor.name}]
                              </>
                            ))}
                          </Group>
                        ))
                    ) : (
                      <span>None</span>
                    )}
                  </Group>
                </Box>
              ))}
            </Group>
          </Box>
        ))}
      </Group>
    </MainContent>
  );
};

export const POOP_GRAPH_QUERY = gql`
  query PoopGraphQuery {
    repositoriesOrError {
      ... on RepositoryConnection {
        nodes {
          __typename
          id
          ... on Repository {
            id
            name
            location {
              id
              name
            }
            pipelines {
              id
              name
              declaredAssetKeys: assetKeys {
                path
              }
              historicalAssetKeys(lastRunCount: 5) {
                path
              }
              sensors {
                id
                name
                isAssetSensor
                assets {
                  id
                  key {
                    path
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`;

// Imported via React.lazy, which requires a default export.
// eslint-disable-next-line import/no-default-export
export default PoopRoot;
