package com.fraud.service;

import com.fraud.model.Transaction;
import com.fraud.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class FraudBlockerService {

    private final TransactionRepository transactionRepository;

    @KafkaListener(
            topics = "fraud-alerts",
            groupId = "fraud-blocker-group",
            properties = {"spring.json.value.default.type=java.lang.String"}
    )
    @Transactional
    public void listenFraudAlerts(String userIdStr) {
        Long userId = Long.parseLong(userIdStr);
        log.warn("[FRAUD ALERT] Received alert for userId: {}. Simulating DB lock...", userId);

        // Находим последнюю транзакцию этого пользователя со статусом NEW
        List<Transaction> transactions = transactionRepository.findAll();

        Transaction latestNewTx = transactions.stream()
                .filter(tx -> tx.getUserId().equals(userId) && tx.getStatus() == Transaction.Status.NEW)
                .max((tx1, tx2) -> tx1.getTimestamp().compareTo(tx2.getTimestamp()))
                .orElse(null);

        if (latestNewTx != null) {
            // Меняем статус на SUSPICIOUS (или CONFIRMED в зависимости от логики)
            latestNewTx.setStatus(Transaction.Status.SUSPICIOUS);
            transactionRepository.save(latestNewTx);
            log.info("🔒 [DB LOCKED] Transaction {} for user {} has been blocked in DB (Status updated to SUSPICIOUS)",
                    latestNewTx.getId(), userId);
        } else {
            log.info("ℹ️ No active NEW transactions found for user {} to block.", userId);
        }
    }
}
