/* package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionKafkaListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "test-group")
    public void handleTransaction(Transaction transaction) {
        // Log the received transaction
        logger.info("Received transaction: {}", transaction);

        // SET YOUR DEBUGGER BREAKPOINT HERE
        // You can inspect transaction.getAmount() to get the amount value

        // For now, just log the amount to make it visible
        logger.info("Transaction amount: {}", transaction.getAmount());
    }
} */

package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionKafkaListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void handleTransaction(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        logger.info("Transaction amount: {}", transaction.getAmount());
        // BREAKPOINT HERE
    }
}
