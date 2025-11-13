import sqlite3
import numpy as np
from pathlib import Path
from typing import Optional
from enum import IntEnum
import re

connection: Optional[sqlite3.Connection] = None


class Language(IntEnum):
    """Language codes for compression."""
    DE = 0x001
    EN = 0x002
    ES = 0x003
    FR = 0x004
    IT = 0x005
    JA = 0x006
    PT = 0x007
    RU = 0x008
    ZH = 0x009
    AR = 0x00A
    FA = 0x00B
    KO = 0x00C
    NL = 0x00D
    PO = 0x00E
    TH = 0x00F
    VI = 0x010
    SEP = 0x000
    UNSPECIFIED = 0xFFFF


def connect(path: str = 'multilang.db') -> sqlite3.Connection:
    """
    Connect to the SQLite database and create tables if needed.

    Args:
        path: Database file path

    Returns:
        SQLite connection object
    """
    global connection
    if connection is None:
        connection = sqlite3.connect(path)
        cursor = connection.cursor()

        # Create words table with composite primary key
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS words ('
            'id INTEGER NOT NULL, '
            'word TEXT NOT NULL, '
            'lang INTEGER NOT NULL, '
            'PRIMARY KEY (id, lang))'
        )

        # Create index for fast lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx ON words(word, lang)')
        connection.commit()
    return connection


def load(source: str, lang: Language) -> None:
    """
    Load words from a text file into the database.

    Args:
        source: Path to source file
        lang: Language enum value
    """
    with open(source, 'r', encoding='utf-8') as file:
        # Parse lines with format: id word
        connect().executemany(
            'INSERT OR REPLACE INTO words VALUES (?, ?, ?)',
            ((int(parts[0]), parts[1].lower(), lang)
             for line in file
             if (parts := line.strip().split()) and len(parts) >= 2)
        )
    connection.commit()


def compress(text: str, lang: Language = Language.EN) -> bytes:
    """
    Compress text by replacing words with their IDs.

    Args:
        text: Input text to compress
        lang: Language of the text

    Returns:
        Compressed bytes with format: [lang(1)][length(4)][ids...][separator][missing words]
    """
    # Tokenize: handle Latin, Cyrillic, CJK characters
    tokens = np.array([
        token.lower()
        for token in re.findall(
            r"[\w']+|[\u4e00-\u9fff]|[\u3040-\u309f]|[\u30a0-\u30ff]|[\uac00-\ud7af]",
            text,
            re.UNICODE
        )
    ])

    if len(tokens) == 0:
        return lang.to_bytes(1, 'big') + b'\x00\x00\x00\x00'

    # Batch query all unique tokens
    lookup = {
        token: row[0]
        for token in np.unique(tokens)
        if (row := connect().cursor().execute(
            'SELECT id FROM words WHERE word = ? AND lang = ?',
            (token, lang)
        ).fetchone()) and row[0] < 65536
    }

    # Build IDs array and find missing positions
    ids = np.array([lookup.get(token, Language.UNSPECIFIED) for token in tokens], dtype=np.uint16)
    missing = ids == Language.UNSPECIFIED

    # Pack result: language byte + length + packed IDs
    packed = ids.tobytes()
    result = lang.to_bytes(1, 'big') + len(packed).to_bytes(4, 'big') + packed

    # Append missing words if any
    if np.any(missing):
        result += b'\x00' + '|'.join(tokens[missing]).encode('utf-8')

    return result


def decompress(data: bytes) -> str:
    """
    Decompress bytes back to text by replacing IDs with words.

    Args:
        data: Compressed bytes

    Returns:
        Decompressed text string
    """
    if len(data) < 5:
        return ''

    length = int.from_bytes(data[1:5], 'big')

    # Handle empty compression
    if length == 0:
        return ' '.join(data[5:].decode('utf-8').split('|')) if len(data) > 5 else ''

    # Extract packed IDs and unspecified words
    packed = data[5:5 + length]
    separator = data.find(b'\x00', 5 + length)

    # Parse missing words if present
    unspec = iter(
        data[separator + 1:].decode('utf-8', errors='ignore').split('|')
        if separator != -1 and separator + 1 < len(data) and data[separator + 1:]
        else []
    )

    # Reconstruct text
    lang = Language(data[0])
    ids = np.frombuffer(packed, dtype=np.uint16)

    # Batch query for known IDs
    lookup = {
        int(row[0]): row[1]
        for val in np.unique(ids[ids != Language.UNSPECIFIED])
        if (row := connect().cursor().execute(
            'SELECT id, word FROM words WHERE id = ? AND lang = ?',
            (int(val), lang)
        ).fetchone())
    }

    # Build word list
    return ' '.join(
        next(unspec, '[MISSING]') if identifier == Language.UNSPECIFIED
        else lookup.get(int(identifier), f'[MISSING:{identifier}]')
        for identifier in ids
    )


if __name__ == '__main__':
    # Language file mappings
    files = {
        Language.EN: 'en.txt',
        Language.RU: 'ru.txt',
        Language.ZH: 'zh.txt',
        Language.JA: 'ja.txt',
        Language.ES: 'es.txt',
        Language.FR: 'fr.txt',
        Language.IT: 'it.txt',
        Language.PT: 'pt.txt',
        Language.DE: 'de.txt',
        Language.AR: 'ar.txt',
        Language.FA: 'fa.txt',
        Language.KO: 'ko.txt',
        Language.NL: 'nl.txt',
        Language.PO: 'po.txt',
        Language.TH: 'th.txt',
        Language.VI: 'vi.txt',
    }

    # Load all language files if database doesn't exist
    if not Path('multilang.db').exists():
        connect()
        for lang, path in files.items():
            if Path(path).exists():
                print(f'Loading {path}...')
                load(path, lang)

    # Test cases for various languages
    tests = [
        ('hi', Language.EN),
        ('hello world', Language.EN),
        ('the quick brown fox jumps over the lazy dog', Language.EN),
        ('compression algorithms are fascinating because they reduce data size while preserving information',
         Language.EN),
        ('Привет мир как дела', Language.RU),
        ('Это тест компрессии', Language.RU),
        ('你好世界', Language.ZH),
        ('这是一个测试', Language.ZH),
        ('こんにちは世界', Language.JA),
        ('これはテストです', Language.JA),
        ('Hola mundo', Language.ES),
        ('La compresión es fascinante', Language.ES),
        ('Bonjour le monde', Language.FR),
        ('Ciao mondo', Language.IT),
        ('Olá mundo', Language.PT),
        ('Hallo Welt', Language.DE),
    ]

    # Run compression tests
    connect()
    for num, (sentence, lang) in enumerate(tests, 1):
        comp = compress(sentence, lang)
        decomp = decompress(comp)
        print(f'Test {num} ({lang.name}): {comp.hex()}')
        print(f'Original: {sentence}')
        print(f'Decompressed: {decomp}')
        print()