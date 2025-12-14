
// =====================================================
// GF2 - SISTEMA DE CONSULTA DE EMPRESAS
// API Backend - VERSAO SIMPLIFICADA POS-MIGRACAO
// =====================================================
const express = require('express');
const mysql = require('mysql2/promise');
const path = require('path');

const app = express();
const port = 3000;

app.get('/favicon.ico', (req, res) => res.status(204).end());

console.log('>>> Iniciando API GF2 V3 - arquivo carregado de:', __dirname);


// -----------------------------------------------------
// CORS / BODY PARSER
// -----------------------------------------------------
app.use(function (req, res, next) {
  const origin = req.headers.origin || '*';
  res.header('Access-Control-Allow-Origin', origin);
  res.header('Vary', 'Origin');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') {
    res.sendStatus(204);
    return;
  }
  next();
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// -----------------------------------------------------
// POOL DE CONEXAO COM MYSQL LOCAL
// -----------------------------------------------------
const dbConfig = {
  host: process.env.DB_HOST || '127.0.0.1',
  port: parseInt(process.env.DB_PORT || '3306', 10),
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'Germ@7525',
  database: process.env.DB_NAME || 'base_cnpj',
  connectionLimit: 5,
  connectTimeout: 10000,
  acquireTimeout: 10000,
  timeout: 30000
};

console.log(`[DB CONFIG] Tentando conectar em ${dbConfig.host}:${dbConfig.port} (database: ${dbConfig.database})`);

const pool = mysql.createPool(dbConfig);

// -----------------------------------------------------
// HELPERS
// -----------------------------------------------------
function aplicarCabecalhosSemCache(res) {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
}

function normalizarUf(uf) {
  if (!uf) return '';
  const texto = String(uf).trim().toUpperCase();
  if (/^[A-Z]{2}$/.test(texto)) {
    return texto;
  }
  return '';
}

function normalizarListaEntrada(valor) {
  if (Array.isArray(valor)) {
    return valor
      .map(function (item) { return String(item || '').trim(); })
      .filter(function (item) { return item !== ''; });
  }
  if (typeof valor === 'string' && valor.trim() !== '') {
    return valor
      .split(',')
      .map(function (item) { return item.trim(); })
      .filter(function (item) { return item !== ''; });
  }
  return [];
}

function normalizarDataConsulta(valor) {
  if (typeof valor !== 'string') return '';
  const texto = valor.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(texto)) {
    return texto;
  }
  return '';
}

function normalizarCnpjNumero(valor) {
  if (valor === null || valor === undefined) return '';
  const apenasNumeros = String(valor).replace(/\D+/g, '');
  if (apenasNumeros.length === 14) {
    return apenasNumeros;
  }
  if (apenasNumeros.length > 14) {
    return apenasNumeros.slice(-14);
  }
  return '';
}

function converterValorNumericoFlex(valor) {
  if (valor === null || valor === undefined) return null;
  if (typeof valor === 'number') {
    return Number.isNaN(valor) ? null : valor;
  }
  const textoOriginal = String(valor).trim();
  if (!textoOriginal) return null;
  const textoNormalizado = textoOriginal
    .replace(/\s+/g, '')
    .replace(/\.(?=\d{3}(\D|$))/g, '')
    .replace(',', '.');
  const numero = Number(textoNormalizado);
  return Number.isNaN(numero) ? null : numero;
}

function chunkArray(array, tamanho) {
  const resultado = [];
  const step = tamanho || 400;
  for (let i = 0; i < array.length; i += step) {
    resultado.push(array.slice(i, i + step));
  }
  return resultado;
}

// -----------------------------------------------------
// ROTA: /municipios  (combo de municipios)
// -----------------------------------------------------
app.get('/municipios', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    const uf = req.query.uf ? String(req.query.uf).trim().toUpperCase() : '';
    const busca = typeof req.query.busca === 'string' ? req.query.busca.trim() : '';

    conn = await pool.getConnection();

    const params = [];
    let sql =
      'SELECT ' +
      '  TRIM(`mun_codigo`) AS codigo, ' +
      '  TRIM(`mun_nome`)   AS descricao, ' +
      '  TRIM(`mun_uf`)     AS uf ' +
      'FROM `municipios` ' +
      'WHERE 1 = 1 ';

    if (uf) {
      sql += 'AND UPPER(`mun_uf`) = ? ';
      params.push(uf);
    }

    if (busca) {
      const termo = '%' + busca + '%';
      sql += 'AND (`mun_nome` LIKE ? OR CAST(`mun_codigo` AS CHAR) LIKE ?) ';
      params.push(termo, termo);
    }

    sql += 'ORDER BY `mun_nome`';

    const resultado = await conn.query(sql, params);
    const rows = Array.isArray(resultado[0]) ? resultado[0] : resultado;

    const lista = rows
      .map(function (row) {
        return {
          codigo: String(row.codigo || '').trim(),
          descricao: String(row.descricao || '').trim(),
          uf: String(row.uf || '').trim()
        };
      })
      .filter(function (item) {
        return item.codigo && item.descricao;
      });

    res.json(lista);
  } catch (err) {
    console.error('Erro /municipios:', err);
    res.status(500).json({ mensagem: 'Erro ao listar municipios', detalhes: err.message });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /cnaes  (combo de CNAE)
// -----------------------------------------------------
app.get('/cnaes', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    const busca = typeof req.query.busca === 'string' ? req.query.busca.trim() : '';

    conn = await pool.getConnection();

    const params = [];
    let sql =
      'SELECT ' +
      '  TRIM(`cna_codigo`)    AS codigo, ' +
      '  TRIM(`cna_descricao`) AS descricao ' +
      'FROM `cnaes` ' +
      'WHERE 1 = 1 ';

    if (busca) {
      const termo = '%' + busca + '%';
      sql += 'AND (`cna_codigo` LIKE ? OR `cna_descricao` LIKE ?) ';
      params.push(termo, termo);
    }

    sql += 'ORDER BY `cna_codigo` LIMIT 200';

    const resultado = await conn.query(sql, params);
    const rows = Array.isArray(resultado[0]) ? resultado[0] : resultado;

    const lista = rows
      .map(function (row) {
        return {
          codigo: String(row.codigo || '').trim(),
          descricao: String(row.descricao || '').trim()
        };
      })
      .filter(function (item) {
        return item.codigo && item.descricao;
      });

    res.json(lista);
  } catch (err) {
    console.error('Erro /cnaes:', err);
    res.status(500).json({ mensagem: 'Erro ao listar CNAEs', detalhes: err.message });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// [NOVO] ROTA: /categorias-mercado
// OBJETIVO: Listar Setores e Segmentos para o filtro em cascata
// ÁREA: Negócios / Classificação (BUS-CLS)
// -----------------------------------------------------

app.get('/categorias-mercado', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    conn = await pool.getConnection();

    const sql = `
      SELECT DISTINCT 
        ind_grande_setor AS setor,
        nom_segmento_mercado AS segmento
      FROM tab_cnae_categorias
      ORDER BY ind_grande_setor, nom_segmento_mercado
    `;

    const [rows] = await conn.query(sql);
    
    // Organiza em formato de árvore: { "Indústria": ["Alimentos", "Metal"], ... }
    const arvore = {};
    rows.forEach(row => {
      if (!arvore[row.setor]) {
        arvore[row.setor] = [];
      }
      if (row.segmento) {
        arvore[row.setor].push(row.segmento);
      }
    });

    res.json(arvore);
  } catch (err) {
    console.error('Erro /categorias-mercado:', err);
    res.status(500).json({ mensagem: 'Erro ao listar categorias', detalhes: err.message });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /situacoes-cadastrais  (combo de situacao cadastral)
// -----------------------------------------------------
app.get('/situacoes-cadastrais', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);

    conn = await pool.getConnection();

    const sql =
      'SELECT ' +
      '  TRIM(`sit_codigo`)    AS codigo, ' +
      '  TRIM(`sit_descricao`) AS descricao ' +
      'FROM `d_situacoes_cadastrais` ' +
      'WHERE 1 = 1 ' +
      'ORDER BY `sit_codigo`';

    const resultado = await conn.query(sql);
    const rows = Array.isArray(resultado[0]) ? resultado[0] : resultado;

    const lista = rows
      .map(function (row) {
        return {
          codigo: String(row.codigo || '').trim(),
          descricao: String(row.descricao || '').trim()
        };
      })
      .filter(function (item) {
        return item.codigo && item.descricao;
      });

    res.json(lista);
  } catch (err) {
    console.error('Erro /situacoes-cadastrais:', err);
    res.status(500).json({
      mensagem: 'Erro ao listar situacoes cadastrais',
      detalhes: err.message
    });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /buscar  (busca principal usada no front)
// -----------------------------------------------------
app.get('/buscar', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);

    const uf = normalizarUf(req.query.uf);
    if (!uf) {
      res.status(400).json({ erro: 'UF invalida. Informe uma sigla com 2 letras.' });
      return;
    }

    // Determina qual view usar baseado na situação cadastral
    const situacoes = normalizarListaEntrada(req.query.situacao);
    const ehEmpresaAtiva = situacoes.length === 0 || situacoes.includes('02') || situacoes.includes('2');
    const tipoView = ehEmpresaAtiva ? 'ativas' : 'inativas';
    const tabelaAlvo = `v_empresas_${tipoView}_${uf.toLowerCase()}`;

    console.log(`[BUSCA] Tipo de empresa: ${tipoView} (situações: ${situacoes.join(', ') || 'todas'})`);

    // Filtros recebidos do Frontend
    const termoBusca = typeof req.query.q === 'string' ? req.query.q.trim() : '';
    const filtroSetor = req.query.setor ? String(req.query.setor).trim() : '';
    const filtroSegmento = req.query.segmento ? String(req.query.segmento).trim() : '';

    const municipios = normalizarListaEntrada(req.query.municipio);
    const cnaesFiltro = normalizarListaEntrada(req.query.cnae).map(function (c) {
      return c.replace(/\D+/g, '');
    }).filter(function (c) { return c !== ''; });
    const portes = normalizarListaEntrada(req.query.porte);

    conn = await pool.getConnection();
    
    // Verifica existência da view
    const [check] = await conn.query(`SHOW TABLES LIKE '${tabelaAlvo}'`);
    if (check.length === 0) {
      return res.status(404).json({ erro: `View não encontrada para ${uf}. Execute o script SQL de atualização.` });
    }

    const params = [];
    const filtros = ['v.uf = ?']; 
    params.push(uf);

    // --- 1. Filtros de Negócio (Setor/Segmento) ---
    if (filtroSetor) {
      filtros.push('cat.ind_grande_setor = ?');
      params.push(filtroSetor);
    }
    if (filtroSegmento) {
      filtros.push('cat.nom_segmento_mercado = ?');
      params.push(filtroSegmento);
    }

    // --- 2. Filtro de Texto (Busca Geral) ---
    if (termoBusca) {
      const termo = '%' + termoBusca + '%';
      filtros.push('(v.razao_social LIKE ? OR v.nome_fantasia LIKE ? OR v.cnpj_completo LIKE ? OR v.bairro LIKE ?)');
      params.push(termo, termo, termo, termo);
    }

    // --- 3. Filtros Específicos ---
    if (municipios.length) {
      const placeholders = municipios.map(function () { return '?'; }).join(', ');
      filtros.push(`v.municipio_codigo IN (${placeholders})`);
      params.push.apply(params, municipios);
    }

    if (cnaesFiltro.length) {
      const clausulas = cnaesFiltro.map(function () {
        return "(REPLACE(REPLACE(REPLACE(v.cnae_fiscal_principal, '.', ''), '/', ''), '-', '') = ?)";
      });
      filtros.push(`(${clausulas.join(' OR ')})`);
      cnaesFiltro.forEach(function (c) {
        params.push(c);
      });
    }

    if (situacoes.length) {
      const placeholders = situacoes.map(function () { return '?'; }).join(', ');
      filtros.push(`v.situacao_inscricao IN (${placeholders})`);
      params.push.apply(params, situacoes);
    }

    if (portes.length) {
      const placeholders = portes.map(function () { return '?'; }).join(', ');
      filtros.push(`v.porte_empresa IN (${placeholders})`); 
      params.push.apply(params, portes);
    }

    // QUERY SQL COMPLETA (Usando crases para evitar erro de sintaxe)
    const sql = `
      SELECT
        v.cnpj_completo,
        v.razao_social,
        v.nome_fantasia,
        v.logradouro,
        v.numero,
        v.complemento,
        v.bairro,
        v.cep,
        v.municipio_codigo,
        v.municipio_nome,
        v.uf,

        v.ddd_1,
        v.telefone_1,
        v.ddd_2,
        v.telefone_2,
        v.email AS correio_eletronico,

        v.cnae_fiscal_principal,
        v.situacao_inscricao AS situacao_cadastral,

        v.motivo_situacao_cadastral,
        mc.mot_descricao AS motivo_situacao_cadastral_descricao,
        sc.sit_descricao AS situacao_cadastral_descricao,

        cat.ind_grande_setor,
        cat.nom_segmento_mercado,

        v.porte_empresa,
        v.capital_social,

        v.latitude AS lat,
        v.longitude AS lon,

        -- Campos de localização geográfica (região, mesorregião, microrregião)
        COALESCE(m.mun_regiao_nome, '') AS regiao_nome,
        COALESCE(m.mun_regiao_sigla, '') AS regiao_sigla,
        COALESCE(dim.mesorregiao_nome, '') AS mesorregiao_nome,
        COALESCE(dim.mesorregiao_id, '') AS mesorregiao_id,
        COALESCE(dim.microrregiao_nome, '') AS microrregiao_nome,
        COALESCE(dim.microrregiao_id, '') AS microrregiao_id,

        -- Campos de dívida ativa
        COALESCE(tb_div.tem_divida, 0) AS tem_divida_ativa,
        COALESCE(tb_div.valor_total, 0) AS valor_divida_ativa_total

      FROM ${tabelaAlvo} v
      -- Joins auxiliares
      LEFT JOIN tab_cnae_categorias cat ON cat.cod_divisao = LEFT(REPLACE(REPLACE(REPLACE(v.cnae_fiscal_principal, '.', ''), '/', ''), '-', ''), 2)
      LEFT JOIN d_motivos_situacao_cadastral mc ON mc.mot_codigo = v.motivo_situacao_cadastral
      LEFT JOIN d_situacoes_cadastrais sc ON sc.sit_codigo = v.situacao_inscricao
      LEFT JOIN municipios m ON m.mun_codigo = v.municipio_codigo
      LEFT JOIN dim_ibge_municipios dim ON dim.ibge_id = v.municipio_codigo
      LEFT JOIN (
        SELECT
          dva_cnpj,
          1 AS tem_divida,
          SUM(dva_valor_consolidado) AS valor_total
        FROM divida_ativa
        GROUP BY dva_cnpj
      ) tb_div ON REPLACE(REPLACE(REPLACE(tb_div.dva_cnpj, '.', ''), '/', ''), '-', '') = REPLACE(REPLACE(REPLACE(v.cnpj_completo, '.', ''), '/', ''), '-', '')

      WHERE ${filtros.join(' AND ')}
      ORDER BY v.razao_social
      LIMIT 2000
    `;

    console.log(`[BUSCA] ========== INICIO DA PESQUISA ==========`);
    console.log(`[BUSCA] UF: ${uf}`);
    console.log(`[BUSCA] Tipo: ${tipoView}`);
    console.log(`[BUSCA] Tabela: ${tabelaAlvo}`);
    console.log(`[BUSCA] Filtros aplicados:`, filtros);
    console.log(`[BUSCA] Parâmetros:`, params);
    console.log(`[BUSCA] SQL (primeiras 500 chars):`, sql.substring(0, 500));
    console.log(`[BUSCA] Iniciando query com timeout de 30s...`);

    const inicio = Date.now();

    // Implementa timeout manual para a query
    const queryPromise = conn.query(sql, params);
    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => reject(new Error('Query timeout após 30 segundos')), 30000)
    );

    const [rows] = await Promise.race([queryPromise, timeoutPromise]);
    const tempo = Date.now() - inicio;

    console.log(`[BUSCA] ✓ Query executada em ${tempo}ms`);
    console.log(`[BUSCA] ✓ Retornou ${rows.length} registros.`);
    if (rows.length > 0) {
      console.log(`[BUSCA] ✓ Primeiro registro (campos):`, Object.keys(rows[0]));
    } else {
      console.log(`[BUSCA] ⚠️ NENHUM registro encontrado!`);
    }
    console.log(`[BUSCA] ✓ Enviando JSON response...`);

    res.json(rows);

    console.log(`[BUSCA] ========== FIM DA PESQUISA ==========`);

  } catch (err) {
    console.error('>>> ERRO SQL NA BUSCA:', err.sqlMessage || err.message);
    res.status(500).json({ erro: 'Erro ao buscar dados', detalhes: err.message });
  } finally {
    if (conn) conn.release();
  }
});
// -----------------------------------------------------
// ROTA: /importacao/cnpjs (Análise de Inativos BLINDADA)
// -----------------------------------------------------
app.post('/importacao/cnpjs', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);

    const ufFiltro = normalizarUf(req.body && req.body.uf);
    const itens = Array.isArray(req.body && req.body.itens) ? req.body.itens : [];

    if (!ufFiltro) return res.status(400).json({ mensagem: 'Selecione uma UF válida.' });
    if (!itens.length) return res.status(400).json({ mensagem: 'Informe ao menos um CNPJ.' });

    // 1. Normalização e Limpeza dos CNPJs
    const mapaEntrada = new Map();
    itens.forEach(item => {
      if (!item) return;
      // Remove não-números e garante 14 dígitos
      let c = String(item.cnpj).replace(/\D+/g, '');
      if (c.length > 0 && c.length < 14) c = c.padStart(14, '0');
      if (c.length > 14) c = c.slice(0, 14);
      
      if (c.length === 14) {
        mapaEntrada.set(c, {
          ...item,
          cnpj: c, // CNPJ limpo para busca
          uf: item.uf || ufFiltro
        });
      }
    });

    const cnpjsConsulta = Array.from(mapaEntrada.keys());
    if (!cnpjsConsulta.length) return res.status(400).json({ mensagem: 'Nenhum CNPJ válido identificado.' });

    conn = await pool.getConnection();
    
    // 2. Diagnóstico da VIEW
    // Tenta descobrir o nome correto da tabela no banco (v_empresas_inativas_rs, _RS, etc)
    const nomeBase = `v_empresas_inativas_${ufFiltro.toLowerCase()}`;
    let nomeTabelaFinal = nomeBase;
    
    // Verifica se a tabela existe
    const [tabelas] = await conn.query(`SHOW TABLES LIKE '${nomeBase}'`);
    if (tabelas.length === 0) {
      // Se não achou, tenta listar parecidas para debug
      const [parecidas] = await conn.query(`SHOW TABLES LIKE 'v_empresas_inativas_%'`);
      const nomesParecidos = parecidas.map(t => Object.values(t)[0]).join(', ');
      
      console.warn(`[ALERTA] View '${nomeBase}' não encontrada. Disponíveis: ${nomesParecidos}`);
      
      // Tenta achar uma que termine com a UF (caso seja maiúscula _RS)
      const match = parecidas.find(t => Object.values(t)[0].match(new RegExp(`_${ufFiltro}$`, 'i')));
      if (match) {
        nomeTabelaFinal = Object.values(match)[0];
        console.log(`[AUTO-CORREÇÃO] Usando tabela encontrada: ${nomeTabelaFinal}`);
      } else {
         return res.status(404).json({ 
           mensagem: `A view de inativos para ${ufFiltro} não foi encontrada no banco.`,
           detalhe: `Esperado: ${nomeBase}. Existentes: ${nomesParecidos || 'Nenhuma'}`
         });
      }
    }

    // 3. Execução da Busca
    const resultadosMapa = new Map();
    const lotes = chunkArray(cnpjsConsulta, 400);

    console.log(`[BUSCA] Consultando ${cnpjsConsulta.length} CNPJs em ${nomeTabelaFinal}`);

    for (const lote of lotes) {
      if (!lote.length) continue;
      const placeholders = lote.map(() => '?').join(', ');
      
      // Tenta buscar usando cnpj_completo (padrão), se der erro, o catch captura
      const sql = `
        SELECT 
          cnpj_completo AS cnpj, 
          razao_social, 
          situacao_cadastral, 
          motivo_situacao_cadastral, 
          municipio_codigo 
        FROM ${nomeTabelaFinal} 
        WHERE cnpj_completo IN (${placeholders})
      `;

      try {
        const [rows] = await conn.query(sql, lote);
        
        rows.forEach(row => {
          let c = String(row.cnpj).replace(/\D+/g, '');
          if (c.length < 14) c = c.padStart(14, '0');
          
          resultadosMapa.set(c, {
            cnpj: c,
            razao_social: row.razao_social,
            orgao: 'RECEITA FEDERAL',
            tipo_situacao_inscricao: row.motivo_situacao_cadastral,
            situacao_inscricao: row.situacao_cadastral,
            receita_principal: 'SITUAÇÃO CADASTRAL IRREGULAR',
            valor_consolidado: 0,
            municipio_codigo: row.municipio_codigo,
            municipio_nome: '',
            uf: ufFiltro
          });
        });
      } catch (err) {
        console.error(`[ERRO SQL LOTE]`, err.message);
        return res.status(500).json({ 
            mensagem: 'Erro ao consultar dados.', 
            detalhe: `Falha na tabela ${nomeTabelaFinal}: ${err.message}` 
        });
      }
    }

    // 4. Montagem da Resposta
    const itensResposta = [];
    let encontrados = 0;

    mapaEntrada.forEach((entrada, cnpj) => {
      const dados = resultadosMapa.get(cnpj);
      if (dados) {
        encontrados++;
        itensResposta.push({
          ...entrada, // Mantém dados originais (codigo, uf informada)
          ...dados,   // Sobrescreve com dados do banco
          status_encontrado: true
        });
      }
    });

    const naoEncontrados = mapaEntrada.size - encontrados;

    res.json({
      quantidade: mapaEntrada.size,
      encontrados,
      nao_encontrados: naoEncontrados,
      tabela_usada: nomeTabelaFinal, // Informação útil para debug no front
      itens: itensResposta
    });

  } catch (err) {
    console.error('[CRITICAL ERROR]', err);
    res.status(500).json({ mensagem: 'Erro crítico no servidor', detalhes: err.message });
  } finally {
    if (conn) conn.release();
  }
});



// -----------------------------------------------------
// ROTA: /divida-ativa/verificar (Consulta Agrupada por CNPJ)
// -----------------------------------------------------
app.post('/divida-ativa/verificar', async function (req, res) {
  let conn;
  try { // <--- ABERTURA DO TRY
    aplicarCabecalhosSemCache(res);

    const itens = Array.isArray(req.body) ? req.body : [];
    if (!itens.length) return res.json({});

    // 1. Normalização
    const cnpjsParaConsulta = new Set();
    itens.forEach(cnpj => {
      const limpo = String(cnpj).replace(/\D+/g, '');
      if (limpo.length === 14) cnpjsParaConsulta.add(limpo);
    });

    const listaCnpjs = Array.from(cnpjsParaConsulta);
    if (!listaCnpjs.length) return res.json({});

    conn = await pool.getConnection();
    const mapaResultados = new Map();
    
    // 2. Processamento em Lotes
    const lotes = chunkArray(listaCnpjs, 500);

    console.log(`[DIVIDA_ATIVA] Unificando dívidas de ${listaCnpjs.length} CNPJs...`);

    for (const lote of lotes) { // <--- ABERTURA DO FOR
      const placeholders = lote.map(() => '?').join(', ');
      
      const sql = 
        'SELECT ' +
        '  dva_cnpj AS cnpj, ' +
        '  MAX(dva_nome_devedor) AS nome_devedor, ' +
        '  GROUP_CONCAT(DISTINCT dva_situacao_inscricao SEPARATOR " / ") AS situacao_unificada, ' +
        '  SUM(dva_valor_consolidado) AS valor_total ' +
        'FROM divida_ativa ' +
        `WHERE dva_cnpj IN (${placeholders}) ` +
        'GROUP BY dva_cnpj';

      const [rows] = await conn.query(sql, lote);
      
      rows.forEach(row => {
        mapaResultados.set(row.cnpj, {
          nome: row.nome_devedor,
          situacao: row.situacao_unificada,
          valor: row.valor_total
        });
      });
    } // <--- FECHAMENTO DO FOR (Verifique se esta chave existe no seu código)

    // 3. Retorna Objeto
    const resposta = Object.fromEntries(mapaResultados);
    res.json(resposta);

  } catch (err) { // <--- FECHAMENTO DO TRY e INÍCIO DO CATCH
    console.error('[DIVIDA_ATIVA] Erro:', err);
    res.status(500).json({ erro: 'Erro ao verificar dívida ativa' });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /geocodificacao/consultar_lote (Recupera Coordenadas)
// -----------------------------------------------------
app.post('/geocodificacao/consultar_lote', async function (req, res) {
  let conn;
  try {
    // 1. Recebe os dados
    const itens = Array.isArray(req.body) ? req.body : [];
    
    console.log(`[GEO LOTE] Recebido pedido para verificar ${itens.length} itens.`);

    // 2. Limpeza Agressiva (Garante que só tenha números)
    const listaCnpjs = itens
      .map(c => String(c).replace(/\D+/g, '')) // Remove ponto, traço, barra
      .filter(c => c.length === 14);          // Garante tamanho certo

    if (!listaCnpjs.length) {
      console.log('[GEO LOTE] Nenhum CNPJ válido após limpeza.');
      return res.json({});
    }

    conn = await pool.getConnection();
    const mapaResultados = {};

    // 3. Processa em lotes de 1000 para não travar o SQL
    const lotes = chunkArray(listaCnpjs, 1000);

    for (const lote of lotes) {
      // Monta os interrogações (?, ?, ?)
      const placeholders = lote.map(() => '?').join(', ');
      
      const sql = `
        SELECT geo_cnpj, geo_latitude, geo_longitude 
        FROM estabelecimentos_geo 
        WHERE geo_cnpj IN (${placeholders})
      `;
      
      const [rows] = await conn.query(sql, lote);
      
      // Preenche o mapa de resposta
      rows.forEach(r => {
        mapaResultados[r.geo_cnpj] = {
          lat: r.geo_latitude,
          lon: r.geo_longitude
        };
      });
    }

    const achados = Object.keys(mapaResultados).length;
    console.log(`[GEO LOTE] Devolvi coordenadas para ${achados} empresas.`);
    
    res.json(mapaResultados);

  } catch (err) {
    console.error('[GEO LOTE] Erro Crítico:', err);
    res.status(500).json({ erro: 'Erro ao recuperar coordenadas' });
  } finally {
    if (conn) conn.release();
  }
});

// ===========================================================
// [BACKEND-GF2] ENDPOINT IBGE – INDICADORES MUNICIPAIS
// ===========================================================

app.get('/ibge/indicadores', async (req, res) => {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);

    const uf = String(req.query.uf || '').trim().toUpperCase();
    if (!uf || uf.length !== 2) {
      return res.status(400).json({ erro: 'Parâmetro "uf" (UF) é obrigatório.' });
    }

    const anoParamRaw = req.query.ano;
    const anoParam = anoParamRaw ? parseInt(anoParamRaw, 10) : null;

    // município pode vir como ?municipio=4314407&municipio=4301602...
    let municipios = [];
    const paramMunicipio = req.query.municipio;
    if (Array.isArray(paramMunicipio)) {
      municipios = paramMunicipio.map(v => String(v).trim()).filter(Boolean);
    } else if (paramMunicipio) {
      municipios = [String(paramMunicipio).trim()];
    }

    conn = await pool.getConnection();

    // 1) Quais anos existem para essa UF?
    const [anosRows] = await conn.query(
      'SELECT DISTINCT ano FROM indicadores_municipais WHERE uf = ? ORDER BY ano',
      [uf]
    );

    if (!anosRows.length) {
      return res.json({
        uf,
        anos_disponiveis: [],
        ano_referencia: null,
        total_municipios: 0,
        indicadores: []
      });
    }

    const anosDisponiveis = anosRows.map(r => r.ano);
    const anoReferencia = anoParam && anosDisponiveis.includes(anoParam)
      ? anoParam
      : anosDisponiveis[anosDisponiveis.length - 1]; // sempre pega o mais recente

    // 2) Puxa indicadores daquele ano
    const params = [uf, anoReferencia];
    let whereExtra = '';

    if (municipios.length) {
      const placeholders = municipios.map(() => '?').join(',');
      whereExtra = ` AND municipio_codigo IN (${placeholders})`;
      params.push(...municipios);
    }

    const [rows] = await conn.query(
      `
      SELECT
        uf,
        municipio_codigo,
        municipio_nome,
        ano,
        populacao_total
      FROM indicadores_municipais
      WHERE uf = ?
        AND ano = ?
        ${whereExtra}
      ORDER BY municipio_nome
      `,
      params
    );

    res.json({
      uf,
      anos_disponiveis: anosDisponiveis,
      ano_referencia: anoReferencia,
      total_municipios: rows.length,
      indicadores: rows
    });
  } catch (error) {
    console.error('[API - /ibge/indicadores] Erro:', error);
    console.error('[API - /ibge/indicadores] Stack:', error.stack);
    res.status(500).json({
      erro: 'Erro ao buscar indicadores municipais do IBGE.',
      detalhe: error.message
    });
  } finally {
    if (conn) conn.release();
  }
});


// =====================================================
// ENDPOINTS DE CACHE DE GEOCODIFICAÇÃO
// =====================================================

// -----------------------------------------------------
// ROTA: /geocodificacao/cache/buscar
// Busca coordenadas no cache antes de consultar API
// -----------------------------------------------------
app.post('/geocodificacao/cache/buscar', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    const itens = Array.isArray(req.body) ? req.body : [];

    if (!itens.length) return res.json({});

    conn = await pool.getConnection();
    const resultados = {};

    // Busca em lote no cache
    for (const item of itens) {
      const cnpj = String(item.cnpj || '').replace(/\D+/g, '');
      const endereco = String(item.endereco || '').trim();

      if (!cnpj || !endereco) continue;

      const [rows] = await conn.query(
        `SELECT latitude, longitude, status_validacao, data_validacao
         FROM cache_geocodificacao
         WHERE cnpj = ? AND endereco_completo = ? AND status_validacao = 'VALIDADO'
         ORDER BY data_validacao DESC
         LIMIT 1`,
        [cnpj, endereco]
      );

      if (rows.length > 0) {
        resultados[cnpj] = {
          lat: rows[0].latitude,
          lon: rows[0].longitude,
          cacheado: true,
          data_validacao: rows[0].data_validacao
        };
      }
    }

    console.log(`[CACHE GEO] Retornou ${Object.keys(resultados).length} coordenadas do cache`);
    res.json(resultados);

  } catch (err) {
    console.error('[CACHE GEO] Erro ao buscar cache:', err);
    res.status(500).json({ erro: 'Erro ao buscar cache de geocodificação' });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /geocodificacao/cache/salvar
// Salva coordenadas validadas no cache
// -----------------------------------------------------
app.post('/geocodificacao/cache/salvar', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    const itens = Array.isArray(req.body) ? req.body : [];

    if (!itens.length) return res.json({ salvos: 0 });

    conn = await pool.getConnection();
    let salvos = 0;

    for (const item of itens) {
      const cnpj = String(item.cnpj || '').replace(/\D+/g, '');
      const endereco = String(item.endereco || '').trim();

      if (!cnpj || !endereco) continue;

      const status = item.lat && item.lon ? 'VALIDADO' : 'NAO_ENCONTRADO';

      await conn.query(
        `INSERT INTO cache_geocodificacao
         (cnpj, endereco_completo, logradouro, numero, bairro, cidade, uf, cep,
          latitude, longitude, status_validacao, data_validacao, fonte)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), 'GOOGLE_API')
         ON DUPLICATE KEY UPDATE
           latitude = VALUES(latitude),
           longitude = VALUES(longitude),
           status_validacao = VALUES(status_validacao),
           data_atualizacao = NOW()`,
        [
          cnpj,
          endereco,
          item.logradouro || null,
          item.numero || null,
          item.bairro || null,
          item.cidade || null,
          item.uf || null,
          item.cep || null,
          item.lat || null,
          item.lon || null,
          status
        ]
      );
      salvos++;
    }

    console.log(`[CACHE GEO] Salvou ${salvos} coordenadas no cache`);
    res.json({ salvos });

  } catch (err) {
    console.error('[CACHE GEO] Erro ao salvar cache:', err);
    res.status(500).json({ erro: 'Erro ao salvar cache de geocodificação' });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /geocodificacao/api/status
// Verifica status de uso da API do mês atual
// -----------------------------------------------------
app.get('/geocodificacao/api/status', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    conn = await pool.getConnection();

    // Garante registro do mês atual
    await conn.query(
      `INSERT IGNORE INTO controle_api_geocoding (ano, mes, total_requisicoes, limite_mensal)
       VALUES (YEAR(NOW()), MONTH(NOW()), 0, 1000)`
    );

    // Busca status
    const [rows] = await conn.query(
      `SELECT
         ano, mes, total_requisicoes, requisicoes_sucesso, requisicoes_erro,
         limite_mensal,
         (limite_mensal - total_requisicoes) AS requisicoes_disponiveis,
         ROUND((total_requisicoes / limite_mensal) * 100, 2) AS percentual_uso,
         CASE
           WHEN total_requisicoes >= limite_mensal THEN 'LIMITE_ATINGIDO'
           WHEN total_requisicoes >= (limite_mensal * 0.9) THEN 'ALERTA'
           ELSE 'DISPONIVEL'
         END AS status_limite,
         data_primeiro_uso, data_ultimo_uso
       FROM controle_api_geocoding
       WHERE ano = YEAR(NOW()) AND mes = MONTH(NOW())`
    );

    res.json(rows[0] || { requisicoes_disponiveis: 1000, status_limite: 'DISPONIVEL' });

  } catch (err) {
    console.error('[API STATUS] Erro:', err);
    res.status(500).json({ erro: 'Erro ao verificar status da API' });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /geocodificacao/api/verificar
// Verifica se pode fazer requisições (limite mensal)
// -----------------------------------------------------
app.get('/geocodificacao/api/verificar', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    conn = await pool.getConnection();

    // Garante registro do mês atual
    await conn.query(
      `INSERT IGNORE INTO controle_api_geocoding (ano, mes, total_requisicoes, limite_mensal)
       VALUES (YEAR(NOW()), MONTH(NOW()), 0, 1000)`
    );

    // Verifica limite
    const [rows] = await conn.query(
      `SELECT total_requisicoes, limite_mensal,
         (limite_mensal - total_requisicoes) AS disponiveis
       FROM controle_api_geocoding
       WHERE ano = YEAR(NOW()) AND mes = MONTH(NOW())`
    );

    const status = rows[0];
    const podeUsar = status.total_requisicoes < status.limite_mensal;

    res.json({
      pode_usar: podeUsar,
      requisicoes_disponiveis: status.disponiveis,
      total_usado: status.total_requisicoes,
      limite_mensal: status.limite_mensal,
      mensagem: podeUsar
        ? `Você pode usar a API. ${status.disponiveis} requisições disponíveis.`
        : `Limite mensal atingido (${status.total_requisicoes}/${status.limite_mensal}). Aguarde o próximo mês.`
    });

  } catch (err) {
    console.error('[API VERIFICAR] Erro:', err);
    res.status(500).json({ erro: 'Erro ao verificar limite da API' });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// ROTA: /geocodificacao/api/registrar
// Registra uso da API (incrementa contador)
// -----------------------------------------------------
app.post('/geocodificacao/api/registrar', async function (req, res) {
  let conn;
  try {
    aplicarCabecalhosSemCache(res);
    const sucesso = req.body.sucesso === true || req.body.sucesso === 'true';

    conn = await pool.getConnection();

    // Garante registro do mês atual
    await conn.query(
      `INSERT IGNORE INTO controle_api_geocoding (ano, mes, total_requisicoes, limite_mensal)
       VALUES (YEAR(NOW()), MONTH(NOW()), 0, 1000)`
    );

    // Incrementa contador
    await conn.query(
      `UPDATE controle_api_geocoding
       SET total_requisicoes = total_requisicoes + 1,
           requisicoes_sucesso = requisicoes_sucesso + ?,
           requisicoes_erro = requisicoes_erro + ?,
           data_ultimo_uso = NOW(),
           data_primeiro_uso = COALESCE(data_primeiro_uso, NOW())
       WHERE ano = YEAR(NOW()) AND mes = MONTH(NOW())`,
      [sucesso ? 1 : 0, sucesso ? 0 : 1]
    );

    res.json({ registrado: true });

  } catch (err) {
    console.error('[API REGISTRAR] Erro:', err);
    res.status(500).json({ erro: 'Erro ao registrar uso da API' });
  } finally {
    if (conn) conn.release();
  }
});

// -----------------------------------------------------
// SERVE FRONTEND LOCAL (index.html no mesmo diretorio)
// -----------------------------------------------------
app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.use(express.static(__dirname));

// -----------------------------------------------------
// START
// -----------------------------------------------------
app.listen(port, function () {
  console.log('API rodando em http://localhost:' + port);
});
