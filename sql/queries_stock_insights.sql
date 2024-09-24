-- Consulta de las tendencias en el precio de cierre por empresa
-- Esta consulta muestra el precio de cierre de cada acción por fecha y empresa
SELECT c.name, sp.date, sp.close
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
ORDER BY c.name, sp.date;  -- Ordenar por nombre de la empresa y fecha

-- Consulta del volumen total negociado por empresa
-- Suma el volumen total de transacciones por empresa, mostrando las compañías con mayor volumen primero
SELECT c.name, SUM(sp.volume) AS total_volume
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
GROUP BY c.name  -- Agrupar por el nombre de la empresa
ORDER BY total_volume DESC;  -- Ordenar por el volumen total en orden descendente

-- Desviación promedio de precios entre apertura y cierre por empresa
-- Calcula la diferencia promedio entre los precios de cierre y apertura para cada empresa
SELECT c.name, AVG(sp.close - sp.open) AS avg_price_change
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
GROUP BY c.name
ORDER BY avg_price_change DESC;  -- Ordenar por la desviación promedio de precio

-- Volatilidad promedio del mercado por empresa
-- Calcula la volatilidad promedio como la diferencia entre los precios más altos y bajos por empresa
SELECT c.name, AVG(sp.high - sp.low) AS avg_volatility
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
GROUP BY c.name
ORDER BY avg_volatility DESC;  -- Ordenar por volatilidad promedio en orden descendente

-- Acciones con el mayor incremento porcentual diario
-- Muestra las acciones con mayor aumento porcentual entre los precios de apertura y cierre
SELECT c.name, sp.date, ((sp.close - sp.open) / sp.open) * 100 AS percentage_change
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
WHERE (sp.close - sp.open) > 0  -- Filtrar solo los días con incrementos
ORDER BY percentage_change DESC;  -- Ordenar por el cambio porcentual en orden descendente

-- Días con el mayor volumen de transacciones para cada acción
-- Muestra los días con mayor volumen de transacciones por empresa (top 10)
SELECT c.name, sp.date, sp.volume
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
ORDER BY sp.volume DESC  -- Ordenar por volumen en orden descendente
LIMIT 10;  -- Mostrar solo los 10 días con mayor volumen

-- Promedio de precios de cierre por mes para cada empresa
-- Agrupa los datos por mes y calcula el precio de cierre promedio para cada empresa
SELECT c.name, DATE_TRUNC('month', sp.date) AS month, AVG(sp.close) AS avg_monthly_close
FROM stock_prices sp
JOIN companies c ON sp.company_id = c.id
GROUP BY c.name, month  -- Agrupar por empresa y mes
ORDER BY month;  -- Ordenar por mes en orden ascendente