package org.apache.kafka.jmh.acl;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 0)
@Measurement(iterations = 4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AuthorizerUpdateBenchmark {
    @Param({"25000", "50000", "75000", "100000"})
    private int aclCount;
    private static final String resourceNamePrefix = "foo-bar35_resource-";
    private static final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private StandardAuthorizer authorizer;
    private final Set<Uuid> ids = new HashSet<>();

    private List<StandardAclWithId> aclsToAdd = prepareAcls();

    int index = 0;
    private static final Random rand = new Random(System.currentTimeMillis());

    @Setup(Level.Trial)
    public void setup() throws Exception {
        authorizer = new StandardAuthorizer();
        addAcls(aclCount);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        authorizer.close();
    }

    @Benchmark
    public void testAddAcl() {
        StandardAclWithId aclWithId = aclsToAdd.get(index++);
        authorizer.addAcl(aclWithId.id(), aclWithId.acl());
    }

    private List<StandardAclWithId> prepareAcls() {
        return IntStream.range(0, 10000)
            .mapToObj(i -> {
                ResourceType resourceType = rand.nextInt(10) > 7? ResourceType.GROUP: ResourceType.TOPIC;
                String resourceName = resourceNamePrefix + i;
                ResourcePattern resourcePattern = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
                return aclsForResource(resourcePattern);
            })
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private List<StandardAclWithId> aclsForResource(ResourcePattern pattern) {
        return IntStream.range(1,256)
            .mapToObj(i -> {
                String p = principal.toString() + rand.nextInt(100);
                String h = "127.0.0." + i;
                return new StandardAcl(pattern.resourceType(), pattern.name(), pattern.patternType(), p, h, READ, ALLOW);
            })
            .map(this::withId)
            .collect(Collectors.toList());
    }

    private StandardAclWithId withId(StandardAcl acl) {
        Uuid id = new Uuid(acl.hashCode(), acl.hashCode());
        while (ids.contains(id)){
            id = Uuid.randomUuid();
        }
        ids.add(id);
        return new StandardAclWithId(id, acl);
    }

    private void addAcls(int num) {
        IntStream.range(0, num)
            .mapToObj(aclsToAdd::get)
            .forEach(aclWithId -> {
                authorizer.addAcl(aclWithId.id(), aclWithId.acl());
                index++;
            });
    }
}