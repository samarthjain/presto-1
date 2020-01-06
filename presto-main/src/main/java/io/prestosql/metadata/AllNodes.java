/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.metadata;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.HostAddress;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AllNodes
{
    private final Set<InternalNode> activeNodes;
    private final Set<InternalNode> inactiveNodes;
    private final Set<InternalNode> shuttingDownNodes;
    private final Set<InternalNode> activeCoordinators;
    private final Cache<HostAddress, Object> blacklist;
    private static final Object DUMMY = new Object();

    public AllNodes(Set<InternalNode> activeNodes, Set<InternalNode> inactiveNodes, Set<InternalNode> shuttingDownNodes, Set<InternalNode> activeCoordinators, long blackListedNodeTimeoutMillis)
    {
        this.activeNodes = ImmutableSet.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
        this.inactiveNodes = ImmutableSet.copyOf(requireNonNull(inactiveNodes, "inactiveNodes is null"));
        this.shuttingDownNodes = ImmutableSet.copyOf(requireNonNull(shuttingDownNodes, "shuttingDownNodes is null"));
        this.activeCoordinators = ImmutableSet.copyOf(requireNonNull(activeCoordinators, "activeCoordinators is null"));
        this.blacklist = CacheBuilder.newBuilder()
                .expireAfterWrite(blackListedNodeTimeoutMillis, TimeUnit.MILLISECONDS)
                .build();
    }

    public Set<InternalNode> getActiveNodes()
    {
        return filterBlackListed(activeNodes);
    }

    public Set<InternalNode> getInactiveNodes()
    {
        return filterBlackListed(inactiveNodes);
    }

    public Set<InternalNode> getShuttingDownNodes()
    {
        return filterBlackListed(shuttingDownNodes);
    }

    public Set<InternalNode> getActiveCoordinators()
    {
        return filterBlackListed(activeCoordinators);
    }

    public Cache<HostAddress, Object> getBlacklist()
    {
        return blacklist;
    }

    public void addBlackListedNode(HostAddress node)
    {
        blacklist.put(node, new Object());
    }

    public Set<InternalNode> filterBlackListed(Set<InternalNode> internalNodes)
    {
        return internalNodes.stream().filter(node -> blacklist.getIfPresent(node.getHostAndPort()) == null).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllNodes allNodes = (AllNodes) o;
        return Objects.equals(activeNodes, allNodes.activeNodes) &&
                Objects.equals(inactiveNodes, allNodes.inactiveNodes) &&
                Objects.equals(shuttingDownNodes, allNodes.shuttingDownNodes) &&
                Objects.equals(activeCoordinators, allNodes.activeCoordinators);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(activeNodes, inactiveNodes, shuttingDownNodes, activeCoordinators);
    }
}
