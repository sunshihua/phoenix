/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(HBaseManagedTimeTest.class)
public class UuidGenFunctionIT extends BaseHBaseManagedTimeIT {
    private void initTable(Connection conn, String sortOrder, String s) throws Exception {
        String ddl = "CREATE TABLE UUID_GEN_TEST (pk VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ", kv VARCHAR)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO UUID_GEN_TEST VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.execute();
        conn.commit();
    }
    
    @Test
    public void testUuidGen() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "DESC", s);

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT uuid_gen() as uuid1, uuid_gen() as uuid2 FROM UUID_GEN_TEST");
        assertTrue(rs.next());
        String uuidString1 = rs.getString(1);
        assertNotNull(uuidString1);
        String uuidString2 = rs.getString(2);
        assertNotNull(uuidString2);
        assertNotEquals(uuidString1, uuidString2);

        // verify the output string is a valid UUID string.
        UUID uuid1 = UUID.fromString(uuidString1);
        assertNotNull(uuid1);
        UUID uuid2 = UUID.fromString(uuidString2);
        assertNotNull(uuid2);
        assertNotEquals(uuid1, uuid2);
    }

    @Test
    public void testUuidGenInQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        initTable(conn, "DESC", s);

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT pk, uuid_gen() as uuid1, uuid_gen() as uuid2 FROM UUID_GEN_TEST");
        assertTrue(rs.next());
        String pkString = rs.getString(1);
        assertEquals(s, pkString);
        String uuidString1 = rs.getString(2);
        assertNotNull(uuidString1);
        String uuidString2 = rs.getString(3);
        assertNotNull(uuidString2);
        assertNotEquals(uuidString1, uuidString2);

        // verify the output string is a valid UUID string.
        UUID uuid1 = UUID.fromString(uuidString1);
        assertNotNull(uuid1);
        UUID uuid2 = UUID.fromString(uuidString2);
        assertNotNull(uuid2);
        assertNotEquals(uuid1, uuid2);
    }                                                           
}
