package org.fg.orchestrator.store;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;

/** fg_operation 行（design §4.2）。 */
public final class OperationRow {

  public enum Status {
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED;

    public boolean terminal() {
      return this != RUNNING;
    }
  }

  public enum Mode {
    HTTPS,
    RELAY;

    public static Mode parse(String s) {
      return valueOf(Objects.requireNonNull(s, "mode").toUpperCase());
    }
  }

  private String queryId;
  private String sessionRef;
  private String sqlHash;
  private String user;
  private String sqlText;
  private String resultKeyPrefix;
  private Mode mode;
  private boolean ordered;
  private byte[] schemaBytes;
  private Status status;
  private String connectOperationId;
  private String attachOwner;
  private OffsetDateTime attachLeaseUntil;
  private String engineRef;
  private String error;
  private OffsetDateTime terminalAt;
  private OffsetDateTime createdAt;
  private OffsetDateTime updatedAt;

  public String queryId() {
    return queryId;
  }

  public OperationRow queryId(String queryId) {
    this.queryId = queryId;
    return this;
  }

  public String sessionRef() {
    return sessionRef;
  }

  public OperationRow sessionRef(String sessionRef) {
    this.sessionRef = sessionRef;
    return this;
  }

  public String sqlHash() {
    return sqlHash;
  }

  public OperationRow sqlHash(String sqlHash) {
    this.sqlHash = sqlHash;
    return this;
  }

  public String user() {
    return user;
  }

  public OperationRow user(String user) {
    this.user = user;
    return this;
  }

  public String sqlText() {
    return sqlText;
  }

  public OperationRow sqlText(String sqlText) {
    this.sqlText = sqlText;
    return this;
  }

  public String resultKeyPrefix() {
    return resultKeyPrefix;
  }

  public OperationRow resultKeyPrefix(String resultKeyPrefix) {
    this.resultKeyPrefix = resultKeyPrefix;
    return this;
  }

  public Mode mode() {
    return mode;
  }

  public OperationRow mode(Mode mode) {
    this.mode = mode;
    return this;
  }

  public boolean ordered() {
    return ordered;
  }

  public OperationRow ordered(boolean ordered) {
    this.ordered = ordered;
    return this;
  }

  public byte[] schemaBytes() {
    return schemaBytes;
  }

  public OperationRow schemaBytes(byte[] schemaBytes) {
    this.schemaBytes = schemaBytes;
    return this;
  }

  public Status status() {
    return status;
  }

  public OperationRow status(Status status) {
    this.status = status;
    return this;
  }

  public String connectOperationId() {
    return connectOperationId;
  }

  public OperationRow connectOperationId(String connectOperationId) {
    this.connectOperationId = connectOperationId;
    return this;
  }

  public String attachOwner() {
    return attachOwner;
  }

  public OperationRow attachOwner(String attachOwner) {
    this.attachOwner = attachOwner;
    return this;
  }

  public OffsetDateTime attachLeaseUntil() {
    return attachLeaseUntil;
  }

  public OperationRow attachLeaseUntil(OffsetDateTime attachLeaseUntil) {
    this.attachLeaseUntil = attachLeaseUntil;
    return this;
  }

  public String engineRef() {
    return engineRef;
  }

  public OperationRow engineRef(String engineRef) {
    this.engineRef = engineRef;
    return this;
  }

  public String error() {
    return error;
  }

  public OperationRow error(String error) {
    this.error = error;
    return this;
  }

  public OffsetDateTime terminalAt() {
    return terminalAt;
  }

  public OperationRow terminalAt(OffsetDateTime terminalAt) {
    this.terminalAt = terminalAt;
    return this;
  }

  public OffsetDateTime createdAt() {
    return createdAt;
  }

  public OperationRow createdAt(OffsetDateTime createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public OffsetDateTime updatedAt() {
    return updatedAt;
  }

  public OperationRow updatedAt(OffsetDateTime updatedAt) {
    this.updatedAt = updatedAt;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OperationRow)) {
      return false;
    }
    OperationRow that = (OperationRow) o;
    return ordered == that.ordered
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(sessionRef, that.sessionRef)
        && Objects.equals(sqlHash, that.sqlHash)
        && Objects.equals(user, that.user)
        && Objects.equals(sqlText, that.sqlText)
        && Objects.equals(resultKeyPrefix, that.resultKeyPrefix)
        && mode == that.mode
        && Arrays.equals(schemaBytes, that.schemaBytes)
        && status == that.status
        && Objects.equals(connectOperationId, that.connectOperationId)
        && Objects.equals(attachOwner, that.attachOwner)
        && Objects.equals(attachLeaseUntil, that.attachLeaseUntil)
        && Objects.equals(engineRef, that.engineRef)
        && Objects.equals(error, that.error)
        && Objects.equals(terminalAt, that.terminalAt)
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(updatedAt, that.updatedAt);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(queryId, sessionRef, sqlHash, user, sqlText, resultKeyPrefix, mode,
        ordered, status, connectOperationId, attachOwner, attachLeaseUntil, engineRef, error,
        terminalAt, createdAt, updatedAt);
    return 31 * result + Arrays.hashCode(schemaBytes);
  }
}
