using System.Text;
using System.Text.RegularExpressions;

namespace HDistributedHybridCache.Infrastructures.Utilities;

/// <summary>
/// Converts a Redis glob-style pattern to an equivalent .NET regex.
/// </summary>
public static class PatternToRegexConverter
{
    /// <summary>
    /// Converts a Redis glob-style pattern (as used by SCAN/KEYS MATCH) into an
    /// equivalent .NET regex. Supports '*', '?', and character classes like
    /// '[abc]', '[^abc]', '[a-z]', as well as backslash-escaped literals.
    /// </summary>
    /// <param name="pattern">The Redis glob-style pattern.</param>
    /// <returns>A compiled .NET regex pattern string.</returns>
    public static string ConvertToRegex(string pattern)
    {
        var sb = new StringBuilder("^");
        var i = 0;
        while (i < pattern.Length)
        {
            var c = pattern[i];
            switch (c)
            {
                case '*':
                    sb.Append(".*");
                    i++;
                    break;
                case '?':
                    sb.Append('.');
                    i++;
                    break;
                case '\\' when i + 1 < pattern.Length:
                    // Redis glob escape: '\x' means literal 'x'
                    sb.Append(Regex.Escape(pattern[i + 1].ToString()));
                    i += 2;
                    break;
                case '[':
                    {
                        // Copy the character class as-is (translating to regex class syntax),
                        // Redis glob classes are already very close to regex classes.
                        var end = pattern.IndexOf(']', i + 1);
                        if (end == -1)
                        {
                            // No closing bracket: treat '[' as a literal.
                            sb.Append(Regex.Escape("["));
                            i++;
                        }
                        else
                        {
                            var classBody = pattern.Substring(i, end - i + 1); // includes [ and ]
                            // Redis uses '^' for negation same as regex; '-' for ranges same as regex.
                            sb.Append(classBody);
                            i = end + 1;
                        }
                        break;
                    }
                default:
                    sb.Append(Regex.Escape(c.ToString()));
                    i++;
                    break;
            }
        }
        sb.Append('$');
        return sb.ToString();
    }
}