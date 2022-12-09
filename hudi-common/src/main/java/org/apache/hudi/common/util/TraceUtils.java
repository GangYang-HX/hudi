package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import java.util.Objects;

/** trace util for meta & file lifetime */
public class TraceUtils {

  /** [TraceType][MetaType][MetaState][TimeLine Version]: [fileDesc] [FileState] */
  public static final String META_TRACE_FORMAT = "[%s][%s][%s]: %s";

  public static final String FILE_STATUS = "%s %s";

  public static final String FILE_STATUS_TRANSFORM = "%s -> %s";

  /** [TraceType][FileType]: [fileDesc] [FileState] */
  public static final String DATA_TRACE_FORMAT = "[%s][%s]: %s";

  public static String getMetaTraceString(
      TraceType traceType,
      MetaType metaType,
      MetaState metaState,
      String fromInstant,
      FileState fromFileState,
      String toInstant,
      FileState toFileState) {

    String instantDesc =
        Objects.isNull(toInstant)
            ? String.format(FILE_STATUS, fromInstant, fromFileState)
            : String.format(
                FILE_STATUS_TRANSFORM,
                String.format(FILE_STATUS, fromInstant, fromFileState),
                String.format(FILE_STATUS, toInstant, toFileState));
    return String.format(META_TRACE_FORMAT, traceType, metaType, metaState, instantDesc);
  }

  public static String getMetaFileCreateTrace(String fromInstant) {
    return getMetaTraceString(
        TraceType.META, MetaType.FILE, MetaState.CREATE, fromInstant, FileState.NEW, null, null);
  }

  public static String getMetaFileForwardTrace(
      String fromInstant, FileState fromFileState, String toInstant, FileState toFileState) {
    return getMetaTraceString(
        TraceType.META,
        MetaType.FILE,
        MetaState.FORWARD,
        fromInstant,
        fromFileState,
        toInstant,
        toFileState);
  }

  public static String getMetaFileRevertTrace(
      String fromInstant, FileState fromFileState, String toInstant, FileState toFileState) {
    return getMetaTraceString(
        TraceType.META,
        MetaType.FILE,
        MetaState.REVERT,
        fromInstant,
        fromFileState,
        toInstant,
        toFileState);
  }

  public static String getMetaFileRevertTrace(String fromInstant, FileState fromFileState) {
    return getMetaTraceString(
        TraceType.META, MetaType.FILE, MetaState.REVERT, fromInstant, fromFileState, null, null);
  }

  public static String getMetaFileRevertOrForwardTrace(
      HoodieInstant fromInstant,
      FileState fromFileState,
      HoodieInstant toInstant,
      FileState toFileState) {

    ValidationUtils.checkState(fromInstant != null && toInstant != null);
    int fromOrdinal = fromInstant.getState().ordinal();
    int toOrdinal = toInstant.getState().ordinal();

    return fromOrdinal <= toOrdinal
        ? getMetaFileForwardTrace(
            fromInstant.getFileName(), fromFileState, toInstant.getFileName(), toFileState)
        : getMetaFileRevertTrace(
            fromInstant.getFileName(), fromFileState, toInstant.getFileName(), toFileState);
  }

  public static String getMetaFileArchiveDeleteTrace(String fromInstant) {
    return getMetaTraceString(
        TraceType.META,
        MetaType.FILE,
        MetaState.ARCHIVE,
        fromInstant,
        FileState.DELETE,
        null,
        null);
  }

  public static String getMetaInstantCreateTrace(String fromInstant) {
    return getMetaTraceString(
        TraceType.META, MetaType.INSTANT, MetaState.CREATE, fromInstant, FileState.NEW, null, null);
  }

  public static String getDataFileTraceString(
      DataFileType dataFileType,
      String fromFile,
      FileState fromFileState,
      String toFile,
      FileState toFileState) {

    String fileDesc =
        Objects.isNull(toFile)
            ? String.format(FILE_STATUS, fromFile, fromFileState)
            : String.format(
                FILE_STATUS_TRANSFORM,
                String.format(FILE_STATUS, fromFile, fromFileState),
                String.format(FILE_STATUS, toFile, toFileState));
    return String.format(DATA_TRACE_FORMAT, TraceType.DATA, dataFileType, fileDesc);
  }

  public static String getDataBaseFileTraceString(
      String fromFile, FileState fromFileState, String toFile, FileState toFileState) {
    return getDataFileTraceString(DataFileType.BASE, fromFile, fromFileState, toFile, toFileState);
  }

  public static String getDataBaseFileCreateTrace(String fromFile) {
    return getDataFileTraceString(DataFileType.BASE, fromFile, FileState.NEW, null, null);
  }

  public static String getDataBaseFileRolloverTrace(String fromFile, String toFile) {
    return getDataFileTraceString(
        DataFileType.BASE, fromFile, FileState.ROLL_OVER, toFile, FileState.NEW);
  }

  public static String getDataBaseFileDeleteTrace(String fromFile) {
    return getDataFileTraceString(DataFileType.BASE, fromFile, FileState.DELETE, null, null);
  }

  public static String getDataFileDeleteTrace(String fromFile) {
    return fromFile.contains(HoodieLogFile.DELTA_EXTENSION)
        ? getDataLogFileTraceString(fromFile, FileState.DELETE, null, null)
        : getDataBaseFileDeleteTrace(fromFile);
  }

  public static String getDataBaseFileRevertTrace(String fromFile, String toFile) {
    return getDataFileTraceString(
        DataFileType.BASE, fromFile, FileState.RENAME_DELETE, toFile, FileState.RENAME_NEW);
  }

  public static String getDataLogFileTraceString(
      String fromFile, FileState fromFileState, String toFile, FileState toFileState) {
    return getDataFileTraceString(DataFileType.LOG, fromFile, fromFileState, toFile, toFileState);
  }

  public static String getDataLogFileCreateTrace(String fromFile) {
    return getDataLogFileTraceString(fromFile, FileState.NEW, null, null);
  }

  public static String getDataLogFileRolloverTrace(String fromFile, String toFile) {
    return getDataLogFileTraceString(fromFile, FileState.ROLL_OVER, toFile, FileState.NEW);
  }

  enum TraceType {
    META,
    DATA,
    INDEX,
    MARKER,
    AUXILIARY
  }

  enum MetaType {
    FILE,
    INSTANT
  }

  enum DataFileType {
    BASE,
    LOG
  }

  enum MetaState {
    CREATE,
    FORWARD,
    REVERT,
    ARCHIVE
  }

  public enum FileState {
    NEW,
    RENAME_NEW,
    DELETE,
    RENAME_DELETE,
    UNCHANGE,
    UNEXIST,
    ROLL_OVER
  }
}
