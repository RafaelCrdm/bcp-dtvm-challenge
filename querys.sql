-- Retorne a quantidade de debêntures listadas no dia anterior.
SELECT COUNT(*) FROM debentures 
WHERE Data = CURDATE() - INTERVAL 1 DAY;

-- Retorne a duration média de todas as debêntures em cada um dos últimos 5 dias úteis
SELECT Data, AVG(Duration) AS duration_media FROM debentures
WHERE Data BETWEEN CURDATE() - INTERVAL 7 DAY AND CURDATE() GROUP BY Data;

-- Busque os códigos únicos de todas as debêntures da empresa “VALE S/A”.
SELECT DISTINCT CodigoFROM debentures
WHERE Nome LIKE '%VALE S/A%';
