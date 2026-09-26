package com.example.saga.participant;

/** A participant-side branch action (forward or compensation). */
@FunctionalInterface
public interface ParticipantAction {
    void execute() throws Exception;
}
