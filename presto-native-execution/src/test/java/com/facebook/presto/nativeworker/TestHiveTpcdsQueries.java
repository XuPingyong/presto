/*
 * Licensed under the Apache License, Version 2.0 (the "License"));
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
package com.facebook.presto.nativeworker;

import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.createSession;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNotNull;

public abstract class TestHiveTpcdsQueries
        extends AbstractTestQueryFramework
{
    public static QueryRunner createNativeQueryRunner(boolean useThrift)
            throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String baseDataDir = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");
        int cacheMaxSize = 0;

        assertNotNull(prestoServerPath);
        assertNotNull(baseDataDir);

        return HiveExternalWorkerQueryRunner.createNativeQueryRunner(baseDataDir, prestoServerPath, Optional.ofNullable(workerCount).map(Integer::parseInt), cacheMaxSize, useThrift);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String baseDataDir = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.of(Paths.get(baseDataDir)));
    }

    private static String getTpcdsQuery(String q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpcds/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceFirst("(?m);$", "");
        return sql;
    }

    // This test runs the 22 TPC-H queries.

    @Test
    public void testTpcdsQ1() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("1"));
    }

    @Test
    public void testTpcdsQ2() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("2"));
    }

    @Test
    public void testTpcdsQ3() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("3"));
    }

    @Test
    public void testTpcdsQ4() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("4"));
    }

    @Test
    public void testTpcdsQ5() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("5"));
    }

    @Test
    public void testTpcdsQ6() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("6"));
    }

    @Test
    public void testTpcdsQ7() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("7"));
    }

    @Test
    public void testTpcdsQ8() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("8"));
    }

    @Test
    public void testTpcdsQ9() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("9"));
    }

    @Test
    public void testTpcdsQ10() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("10"));
    }

    @Test
    public void testTpcdsQ11() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("11"));
    }

    @Test
    public void testTpcdsQ12() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("12"));
    }

    @Test
    public void testTpcdsQ13() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("13"));
    }

    @Test
    public void testTpcdsQ14a() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("14a"));
    }

    @Test
    public void testTpcdsQ14b() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("14b"));
    }

    @Test
    public void testTpcdsQ15() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("15"));
    }

    @Test
    public void testTpcdsQ16() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("16"));
    }

    @Test
    public void testTpcdsQ17() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("17"));
    }

    @Test
    public void testTpcdsQ18() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("18"));
    }

    @Test
    public void testTpcdsQ19() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("19"));
    }

    @Test
    public void testTpcdsQ20() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("20"));
    }

    @Test
    public void testTpcdsQ21() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("21"));
    }

    @Test
    public void testTpcdsQ22() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("22"));
    }

    @Test
    public void testTpcdsQ23a() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("23a"));
    }

    @Test
    public void testTpcdsQ23b() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("23b"));
    }

    @Test
    public void testTpcdsQ24a() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("24a"));
    }

    @Test
    public void testTpcdsQ24b() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("24b"));
    }

    @Test
    public void testTpcdsQ25() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("25"));
    }

    @Test
    public void testTpcdsQ26() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("26"));
    }

    @Test
    public void testTpcdsQ27() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("27"));
    }

    @Test
    public void testTpcdsQ28() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("28"));
    }

    @Test
    public void testTpcdsQ29() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("29"));
    }

    @Test
    public void testTpcdsQ30() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("30"));
    }

    @Test
    public void testTpcdsQ31() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("31"));
    }

    @Test
    public void testTpcdsQ32() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("32"));
    }

    @Test
    public void testTpcdsQ33() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("33"));
    }

    @Test
    public void testTpcdsQ34() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("34"));
    }

    @Test
    public void testTpcdsQ35() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("35"));
    }

    @Test
    public void testTpcdsQ36() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("36"));
    }

    @Test
    public void testTpcdsQ37() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("37"));
    }

    @Test
    public void testTpcdsQ38() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("38"));
    }

    @Test
    public void testTpcdsQ39() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("39"));
    }

    @Test
    public void testTpcdsQ40() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("40"));
    }

    @Test
    public void testTpcdsQ41() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("41"));
    }

    @Test
    public void testTpcdsQ42() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("42"));
    }

    @Test
    public void testTpcdsQ43() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("43"));
    }

    @Test
    public void testTpcdsQ44() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("44"));
    }

    @Test
    public void testTpcdsQ45() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("45"));
    }

    @Test
    public void testTpcdsQ46() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("46"));
    }

    @Test
    public void testTpcdsQ47() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("47"));
    }

    @Test
    public void testTpcdsQ48() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("48"));
    }

    @Test
    public void testTpcdsQ49() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("49"));
    }

    @Test
    public void testTpcdsQ50() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("50"));
    }

    @Test
    public void testTpcdsQ51() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("51"));
    }

    @Test
    public void testTpcdsQ52() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("52"));
    }

    @Test
    public void testTpcdsQ53() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("53"));
    }

    @Test
    public void testTpcdsQ54() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("54"));
    }

    @Test
    public void testTpcdsQ55() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("55"));
    }

    @Test
    public void testTpcdsQ56() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("56"));
    }

    @Test
    public void testTpcdsQ57() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("57"));
    }

    @Test
    public void testTpcdsQ58() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("58"));
    }

    @Test
    public void testTpcdsQ59() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("59"));
    }

    @Test
    public void testTpcdsQ60() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("60"));
    }

    @Test
    public void testTpcdsQ61() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("61"));
    }

    @Test
    public void testTpcdsQ62() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("62"));
    }

    @Test
    public void testTpcdsQ63() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("63"));
    }

    @Test
    public void testTpcdsQ64() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("64"));
    }

    @Test
    public void testTpcdsQ65() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("65"));
    }

    @Test
    public void testTpcdsQ66() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("66"));
    }

    @Test
    public void testTpcdsQ67() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("67"));
    }

    @Test
    public void testTpcdsQ68() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("68"));
    }

    @Test
    public void testTpcdsQ69() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("69"));
    }

    @Test
    public void testTpcdsQ70() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("70"));
    }

    @Test
    public void testTpcdsQ71() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("71"));
    }

    @Test
    public void testTpcdsQ72() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("72"));
    }

    @Test
    public void testTpcdsQ73() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("73"));
    }

    @Test
    public void testTpcdsQ74() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("74"));
    }

    @Test
    public void testTpcdsQ75() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("75"));
    }

    @Test
    public void testTpcdsQ76() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("76"));
    }

    @Test
    public void testTpcdsQ77() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("77"));
    }

    @Test
    public void testTpcdsQ78() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("78"));
    }

    @Test
    public void testTpcdsQ79() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("79"));
    }

    @Test
    public void testTpcdsQ80() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("80"));
    }

    @Test
    public void testTpcdsQ81() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("81"));
    }

    @Test
    public void testTpcdsQ82() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("82"));
    }

    @Test
    public void testTpcdsQ83() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("83"));
    }

    @Test
    public void testTpcdsQ84() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("84"));
    }

    @Test
    public void testTpcdsQ85() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("85"));
    }

    @Test
    public void testTpcdsQ86() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("86"));
    }

    @Test
    public void testTpcdsQ87() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("87"));
    }

    @Test
    public void testTpcdsQ88() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("88"));
    }

    @Test
    public void testTpcdsQ89() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("89"));
    }

    @Test
    public void testTpcdsQ90() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("90"));
    }

    @Test
    public void testTpcdsQ91() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("91"));
    }

    @Test
    public void testTpcdsQ92() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("92"));
    }

    @Test
    public void testTpcdsQ93() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("93"));
    }

    @Test
    public void testTpcdsQ94() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("94"));
    }

    @Test
    public void testTpcdsQ95() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("95"));
    }

    @Test
    public void testTpcdsQ96() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("96"));
    }

    @Test
    public void testTpcdsQ97() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("97"));
    }

    @Test
    public void testTpcdsQ98() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("98"));
    }

    @Test
    public void testTpcdsQ99() throws Exception
    {
        assertQuerySucceeds(createSession(Optional.empty(), "tpcds"), getTpcdsQuery("99"));
    }
}
