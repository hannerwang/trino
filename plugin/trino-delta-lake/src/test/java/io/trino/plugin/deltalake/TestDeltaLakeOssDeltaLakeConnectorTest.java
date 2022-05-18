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
package io.trino.plugin.deltalake;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeOssDeltaLakeConnectorTest
        extends BaseDeltaLakeMinioConnectorTest
{
    public TestDeltaLakeOssDeltaLakeConnectorTest()
    {
        super("ossdeltalake-test-queries", "io/trino/plugin/deltalake/testing/resources/ossdeltalake/");
    }

    @Override
    public void testShowCreateTable()
    {
        // Table comment is different from TestDeltaLakeDatabricksConnectorTest
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE delta_lake.test_schema.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = 's3://ossdeltalake-test-queries/test_schema/orders',\n" +
                        "   partitioned_by = ARRAY[]\n" +
                        ")");
    }
}
