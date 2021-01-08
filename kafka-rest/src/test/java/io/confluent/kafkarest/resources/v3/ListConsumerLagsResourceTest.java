/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.resources.v3;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ConsumerLagManager;
import io.confluent.kafkarest.entities.ConsumerLag;
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.ConsumerLagDataList;
import io.confluent.kafkarest.entities.v3.ListConsumerLagsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.List;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ListConsumerLagsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";

  private static final ConsumerLag CONSUMER_LAG_1 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName("topic-1")
          .setPartitionId(1)
          .setConsumerId("consumer-1")
          .setInstanceId("instance-1")
          .setClientId("client-1")
          .setCurrentOffset(100L)
          .setLogEndOffset(101L)
          .build();

  private static final ConsumerLag CONSUMER_LAG_2 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName("topic-1")
          .setPartitionId(2)
          .setConsumerId("consumer-2")
          .setInstanceId("instance-2")
          .setClientId("client-2")
          .setCurrentOffset(100L)
          .setLogEndOffset(200L)
          .build();

  private static final List<ConsumerLag> CONSUMER_LAG_LIST = Arrays
      .asList(CONSUMER_LAG_1, CONSUMER_LAG_2);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerLagManager consumerLagManager;

  private ListConsumerLagsResource consumerLagResource;

  @Before
  public void setUp() {
    consumerLagResource =
        new ListConsumerLagsResource(
            () -> consumerLagManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void listConsumerLags_returnsConsumerLags() {
    expect(consumerLagManager.listConsumerLags(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(CONSUMER_LAG_LIST));
    replay(consumerLagManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerLagResource.listConsumerLags(response, CLUSTER_ID, CONSUMER_GROUP_ID);

    ListConsumerLagsResponse expected =
        ListConsumerLagsResponse.create(
            ConsumerLagDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1/lags")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerLagData.fromConsumerLag(CONSUMER_LAG_2)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/2/lags/consumer-group-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=2/lag=consumer-group-1")
                                    .build())
                            .build(),
                        ConsumerLagData.fromConsumerLag(CONSUMER_LAG_1)
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1/lags/consumer-group-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=1/lag=consumer-group-1")
                                    .build())
                            .build()))
            .build());

    assertEquals(expected, response.getValue());
  }
}
