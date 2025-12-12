package tech.ydb.spark.connector.impl;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryParser {
    private static final String SCHEME_PREFIX = "SELECT * FROM (";
    private static final String SCHEME_SUFFIX = ") WHERE 1 = 0";

    private QueryParser() { }

    public static String describeYQL(String origin) {
        StringBuilder schemeQuery = new StringBuilder();

        int parenLevel = 0;
        int fragmentStart = 0;
        int statementStart = -1;
        int keywordStart = -1;

        boolean statementIsSelect = false;
        int selectStatementsCount = 0;
        int otherStatementCount = 0;


        char[] chars = origin.toCharArray();

        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i];
            boolean isInsideKeyword = false;

            int keywordEnd = i; // parseSingleQuotes, parseDoubleQuotes, etc move index so we keep old value
            switch (ch) {
                case '\'': // single-quotes
                    i = parseSingleQuotes(chars, i);
                    break;

                case '"': // double-quotes
                    i = parseDoubleQuotes(chars, i);
                    break;

                case '`': // backtick-quotes
                    i = parseBacktickQuotes(chars, i);
                    break;

                case '-': // possibly -- style comment
                    i = parseLineComment(chars, i);
                    break;

                case '/': // possibly /* */ style comment
                    i = parseBlockComment(chars, i);
                    break;

                default:
                    if (keywordStart >= 0) {
                        isInsideKeyword = Character.isJavaIdentifierPart(ch);
                        break;
                    }
                    // Not in keyword, so just detect next keyword start
                    isInsideKeyword = Character.isJavaIdentifierStart(ch);
                    if (isInsideKeyword) {
                        keywordStart = i;
                    }
                    break;
            }


            if (keywordStart >= 0 && (!isInsideKeyword || (i == chars.length - 1))) {
                int keywordLength = (isInsideKeyword ? i + 1 : keywordEnd) - keywordStart;

                if (statementStart < 0) {
                    // Detecting type of statement by the first keyword
                    statementStart = keywordStart;

                    // starts with SELECT
                    if (parseSelectKeyword(chars, keywordStart, keywordLength)) {
                        statementIsSelect = true;
                        selectStatementsCount++;
                        schemeQuery.append(chars, fragmentStart, keywordStart - fragmentStart);
                        schemeQuery.append(SCHEME_PREFIX);
                        fragmentStart = keywordStart;
                    } else {
                        statementIsSelect = false;
                    }

                    // Any other statement
                    if (parseInsertKeyword(chars, keywordStart, keywordLength)
                            || parseUpsertKeyword(chars, keywordStart, keywordLength)
                            || parseUpdateKeyword(chars, keywordStart, keywordLength)
                            || parseDeleteKeyword(chars, keywordStart, keywordLength)
                            || parseReplaceKeyword(chars, keywordStart, keywordLength)
                            || parseAlterKeyword(chars, keywordStart, keywordLength)
                            || parseCreateKeyword(chars, keywordStart, keywordLength)
                            || parseDropKeyword(chars, keywordStart, keywordLength)
                            || parseGrantKeyword(chars, keywordStart, keywordLength)
                            || parseRevokeKeyword(chars, keywordStart, keywordLength)) {
                        otherStatementCount++;
                    }
                }

                keywordStart = -1;
            }

            switch (ch) {
                case '(':
                    parenLevel++;
                    break;
                case ')':
                    parenLevel--;
                    break;
                case ';':
                    if (parenLevel == 0) {
                        if (statementIsSelect) {
                            schemeQuery.append(chars, fragmentStart, i - fragmentStart);
                            schemeQuery.append(SCHEME_SUFFIX);
                            fragmentStart = i;
                            statementIsSelect = false;
                        }
                        statementStart = -1;
                    }
                    break;
                default:
                    // nothing
                    break;
            }
        }

        if (fragmentStart < chars.length) {
            schemeQuery.append(chars, fragmentStart, chars.length - fragmentStart);
            if (statementIsSelect) {
                schemeQuery.append(SCHEME_SUFFIX);
            }
        }

        if (selectStatementsCount == 1 && otherStatementCount == 0) {
            return schemeQuery.toString();
        }

        return null;
    }

    private static int parseSingleQuotes(final char[] query, int offset) {
        // treat backslashes as escape characters
        while (++offset < query.length) {
            switch (query[offset]) {
                case '\\':
                    ++offset;
                    break;
                case '\'':
                    return offset;
                default:
                    break;
            }
        }

        return query.length;
    }

    @SuppressWarnings("EmptyBlock")
    private static int parseDoubleQuotes(final char[] query, int offset) {
        while (++offset < query.length && query[offset] != '"') {
            // do nothing
        }
        return offset;
    }

    @SuppressWarnings("EmptyBlock")
    private static int parseBacktickQuotes(final char[] query, int offset) {
        while (++offset < query.length && query[offset] != '`') {
            // do nothing
        }
        return offset;
    }

    private static int parseLineComment(final char[] query, int offset) {
        if (offset + 1 < query.length && query[offset + 1] == '-') {
            while (offset + 1 < query.length) {
                offset++;
                if (query[offset] == '\r' || query[offset] == '\n') {
                    break;
                }
            }
        }
        return offset;
    }

    private static int parseBlockComment(final char[] query, int offset) {
        if (offset + 1 < query.length && query[offset + 1] == '*') {
            // /* /* */ */ nest, according to SQL spec
            int level = 1;
            for (offset += 2; offset < query.length; ++offset) {
                switch (query[offset - 1]) {
                    case '*':
                        if (query[offset] == '/') {
                            --level;
                            ++offset; // don't parse / in */* twice
                        }
                        break;
                    case '/':
                        if (query[offset] == '*') {
                            ++level;
                            ++offset; // don't parse * in /*/ twice
                        }
                        break;
                    default:
                        break;
                }

                if (level == 0) {
                    --offset; // reset position to last '/' char
                    break;
                }
            }
        }
        return offset;
    }

    private static boolean parseAlterKeyword(char[] query, int offset, int length) {
        if (length != 5) {
            return false;
        }

        return (query[offset] | 32) == 'a'
                && (query[offset + 1] | 32) == 'l'
                && (query[offset + 2] | 32) == 't'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r';
    }

    private static boolean parseCreateKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'c'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'e'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseDropKeyword(char[] query, int offset, int length) {
        if (length != 4) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'o'
                && (query[offset + 3] | 32) == 'p';
    }

    private static boolean parseGrantKeyword(char[] query, int offset, int length) {
        if (length != 5) {
            return false;
        }

        return (query[offset] | 32) == 'g'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'a'
                && (query[offset + 3] | 32) == 'n'
                && (query[offset + 4] | 32) == 't';
    }

    private static boolean parseRevokeKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'r'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'v'
                && (query[offset + 3] | 32) == 'o'
                && (query[offset + 4] | 32) == 'k'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseSelectKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'c'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseUpdateKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 'd'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseUpsertKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseInsertKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'i'
                && (query[offset + 1] | 32) == 'n'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseDeleteKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseReplaceKeyword(char[] query, int offset, int length) {
        if (length != 7) {
            return false;
        }

        return (query[offset] | 32) == 'r'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'p'
                && (query[offset + 3] | 32) == 'l'
                && (query[offset + 4] | 32) == 'a'
                && (query[offset + 5] | 32) == 'c'
                && (query[offset + 6] | 32) == 'e';
    }
}
