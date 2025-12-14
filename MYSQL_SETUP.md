# Configuração do MySQL para Base_CNPJ

## Problema Identificado

O servidor Node.js não consegue conectar ao MySQL porque **o servidor MySQL não está rodando** ou está em uma porta diferente.

**Erro atual:**
```
ECONNREFUSED 127.0.0.1:3306
```

## Solução

### Opção 1: Iniciar o MySQL (Recomendado)

Se você tem o MySQL instalado localmente, inicie o serviço:

```bash
# No Linux (systemd)
sudo systemctl start mysql
sudo systemctl enable mysql  # Para iniciar automaticamente

# No Linux (service)
sudo service mysql start

# No macOS
brew services start mysql

# No Windows
net start MySQL80  # ou nome do seu serviço MySQL
```

Verifique se o MySQL está rodando:
```bash
mysql -u root -p
```

### Opção 2: Usar Docker

Se preferir rodar o MySQL em um contêiner Docker:

```bash
docker run -d \
  --name mysql-base-cnpj \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=Germ@7525 \
  -e MYSQL_DATABASE=base_cnpj \
  mysql:8.0
```

### Opção 3: Configurar Porta Diferente

Se seu MySQL está rodando em uma porta diferente (ex: 3307), configure via variáveis de ambiente:

```bash
# Linux/macOS
export DB_PORT=3307
npm start

# Windows (PowerShell)
$env:DB_PORT="3307"
npm start

# Windows (CMD)
set DB_PORT=3307
npm start
```

## Variáveis de Ambiente Disponíveis

O servidor agora suporta configuração via variáveis de ambiente:

| Variável | Padrão | Descrição |
|----------|---------|-----------|
| `DB_HOST` | `127.0.0.1` | Host do MySQL |
| `DB_PORT` | `3306` | Porta do MySQL |
| `DB_USER` | `root` | Usuário do MySQL |
| `DB_PASSWORD` | `Germ@7525` | Senha do MySQL |
| `DB_NAME` | `base_cnpj` | Nome do banco de dados |

## Verificar Status do MySQL

```bash
# Ver se o MySQL está escutando
netstat -tlnp | grep 3306
# ou
ss -tlnp | grep 3306

# Ver processos MySQL rodando
ps aux | grep mysql
```

## Após Iniciar o MySQL

1. Reinicie o servidor Node.js:
   ```bash
   pkill -f "node index.js"
   npm start
   ```

2. Teste a conexão:
   ```bash
   curl http://localhost:3000/situacoes-cadastrais
   ```

3. Se retornar dados (array JSON), a conexão está funcionando! ✅

4. Agora a pesquisa de CNPJ deve funcionar normalmente no frontend.

## Importar o Banco de Dados

Certifique-se de que o banco `base_cnpj` existe e contém as tabelas necessárias:

```bash
mysql -u root -p < seu_dump.sql
```

Ou crie o banco manualmente:
```sql
CREATE DATABASE IF NOT EXISTS base_cnpj CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```
