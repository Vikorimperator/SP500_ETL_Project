-- Crear la tabla `companies` si no existe
-- Esta tabla almacena las compañías con un identificador único (id) y su nombre (name)
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,  -- Identificador único para cada compañía
    name VARCHAR(10) NOT NULL UNIQUE  -- Nombre de la compañía, debe ser único
);

-- Crear la tabla `stock_prices` si no existe
-- Esta tabla almacena los precios históricos de las acciones, vinculando cada registro con una compañía
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,  -- Identificador único para cada registro de precio de acción
    company_id INTEGER REFERENCES companies(id),  -- Identificador de la compañía, referenciado desde la tabla `companies`
    date DATE NOT NULL,  -- Fecha del registro de precio
    open NUMERIC(10, 2),  -- Precio de apertura de la acción
    high NUMERIC(10, 2),  -- Precio más alto de la acción en esa fecha
    low NUMERIC(10, 2),   -- Precio más bajo de la acción en esa fecha
    close NUMERIC(10, 2), -- Precio de cierre de la acción
    volume BIGINT  -- Volumen de transacciones de la acción en esa fecha
);
