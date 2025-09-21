package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionKafkaListener.class);
    private final DatabaseConduit databaseConduit;

    public TransactionKafkaListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void handleTransaction(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        logger.info("Transaction amount: {}", transaction.getAmount());

        // Validate the transaction
        if (isValidTransaction(transaction)) {
            processTransaction(transaction);
            logger.info("Transaction processed successfully");
        } else {
            logger.warn("Transaction validation failed, discarding: {}", transaction);
        }
    }

    private boolean isValidTransaction(Transaction transaction) {
        // Check if sender exists and is valid
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Invalid senderId: {}", transaction.getSenderId());
            return false;
        }

        // Check if recipient exists and is valid
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Invalid recipientId: {}", transaction.getRecipientId());
            return false;
        }

        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance. Required: {}, Available: {}",
                    transaction.getAmount(), sender.getBalance());
            return false;
        }

        return true;
    }

    private void processTransaction(Transaction transaction) {
        // Get sender and recipient
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save updated users
        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        // Create and save transaction record
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
        databaseConduit.save(transactionRecord);

        logger.info("Updated sender {} balance to: {}", sender.getName(), sender.getBalance());
        logger.info("Updated recipient {} balance to: {}", recipient.getName(), recipient.getBalance());
    }
}