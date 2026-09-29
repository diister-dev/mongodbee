import { test } from "../../library/test/+harness.ts";
import { assertEquals } from "../../library/test/+assert.ts";
import { mongoHosts } from "../src/command.ts";

test("mongoHosts shows where the studio connects without the credentials", () => {
  assertEquals(mongoHosts("mongodb://localhost:27017"), "localhost:27017");
  assertEquals(
    mongoHosts("mongodb://admin:s3cr@t@db1:27017,db2:27017/app?replicaSet=rs0"),
    "db1:27017,db2:27017",
  );
  assertEquals(
    mongoHosts(
      "mongodb+srv://user:pass@cluster0.example.net/?retryWrites=true",
    ),
    "cluster0.example.net",
  );
});
