# Use a imagem base do PostgreSQL
FROM postgres:16.0

# Environmental variables
ENV PGDATA=/var/lib/postgresql/data
ENV POSTGRES_PASSWORD=mysecretpassword

# Copie seu arquivo SQL para o diretório de inicialização
COPY /demo-big-en/demo-big-en-20170815.sql /docker-entrypoint-initdb.d/

# Exponha a porta padrão do PostgreSQL (opcional)
EXPOSE 5432
