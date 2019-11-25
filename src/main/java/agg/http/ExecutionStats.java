package agg.http;

import org.apache.jena.ext.com.google.common.math.Stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * An utility class used to record statistic during query execution
 * @author Thomas Minier
 */
public class ExecutionStats {
    private double executionTime;
    private int nbCallsRead;
    private int nbCallsWrite;
    private boolean enableLogs = false;
    private List<Double> decodingResponses;
    private List<String> logs;
    private List<Double> transferSizes;
    private List<Double> httpTimesRead;
    private List<Double> httpTimesWrite;
    private List<Double> resumeTimesRead;
    private List<Double> suspendTimesRead;
    private List<Double> resumeTimesWrite;
    private List<Double> suspendTimesWrite;
    private List<Double> nextNumber;
    private List<Double> nextOptimizedNumber;
    private List<Double> planSize;

    public ExecutionStats() {
        executionTime = -1;
        nbCallsRead = 0;
        nbCallsWrite = 0;
        logs = new ArrayList<>();
        decodingResponses = new ArrayList<>();
        httpTimesRead = new ArrayList<>();
        httpTimesWrite = new ArrayList<>();
        resumeTimesRead = new ArrayList<>();
        suspendTimesRead = new ArrayList<>();
        resumeTimesWrite = new LinkedList<>();
        suspendTimesWrite = new LinkedList<>();
        transferSizes = new ArrayList<>();
        nextNumber = new ArrayList<>();
        nextOptimizedNumber = new ArrayList<>();
        planSize = new ArrayList<>();
    }

    public double getMeanNextNumber() {
        if (nextNumber.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(nextNumber);
    }

    public double getMeanNextNumberOptimized() {
        if (nextOptimizedNumber.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(nextOptimizedNumber);
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public int getNbCallsRead() {
        return nbCallsRead;
    }

    public int getNbCallsWrite() {
        return nbCallsWrite;
    }

    public Double getMeanHTTPTimesRead() {
        if (httpTimesRead.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(httpTimesRead);
    }

    public Double getMeanHTTPTimesWrite() {
        if (httpTimesWrite.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(httpTimesWrite);
    }

    public Double getMeanResumeTimeRead() {
        if (resumeTimesRead.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(resumeTimesRead);
    }

    public Double getMeanSuspendTimeRead() {
        if (suspendTimesRead.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(suspendTimesRead);
    }

    public Double getMeanResumeTimeWrite() {
        if (resumeTimesWrite.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(resumeTimesWrite);
    }

    public Double getMeanSuspendTimeWrite() {
        if (suspendTimesWrite.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(suspendTimesWrite);
    }

    public Double getMeanTransferSize() {
        if (transferSizes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(transferSizes);
    }

    public Double getTotalTransferSize() {
        if (transferSizes.isEmpty()) {
            return 0.0;
        }
        return transferSizes.parallelStream().reduce(0.0, (a, b) -> a + b);
    }

    public void startTimer() {
        executionTime = System.nanoTime();
    }

    public void stopTimer() {
        double endTime = System.nanoTime();
        executionTime = (endTime - executionTime) / 1e9;
    }

    public void reportHTTPQueryRead(double execTime) {
        nbCallsRead++;
        httpTimesRead.add(execTime);
    }

    public void reportHTTPQueryWrite(double execTime) {
        nbCallsWrite++;
        httpTimesWrite.add(execTime);
    }

    public void reportOverheadRead(double resumeTime, double suspendTime) {
        resumeTimesRead.add(resumeTime);
        suspendTimesRead.add(suspendTime);
    }

    public void reportOverheadWrite(double resumeTime, double suspendTime) {
        resumeTimesWrite.add(resumeTime);
        suspendTimesWrite.add(suspendTime);
    }

    public void reportNextNumbers(double next, double nextOpt) {
        nextNumber.add(next);
        nextOptimizedNumber.add(nextOpt);
    }

    public void reportTransferSize(double size) {
        transferSizes.add(size);
    }

    public void setLogs(boolean b) {
        this.enableLogs = true;
    }

    public void reportLogs(String log) {
        if(this.enableLogs) System.err.println(log);
        this.logs.add(log);
    }

    public void reportDecodingResponseTime(double decodingTimeEnd) {
        decodingResponses.add(decodingTimeEnd);
    }

    public double getMeanDecodingResponseTime() {
        if (decodingResponses.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(decodingResponses);
    }

    public double getTotalPlanSize() {
        if (planSize.isEmpty()) {
            return 0.0;
        }
        return planSize.parallelStream().reduce(0.0, (a, b) -> a + b);
    }

    public void reportPlanSize(double planSize) {
        this.planSize.add(planSize);
    }
}
