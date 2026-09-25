package io.saga.participant;

/** Business action executed by a participant under barrier protection. */
@FunctionalInterface
public interface ParticipantAction {
    void run() throws Exception;
}
