/**
 * @fileoverview CLI utility functions
 * 
 * Provides utility functions for CLI operations such as text formatting.
 * 
 * @module
 */

/**
 * Formats multi-line text by removing leading/trailing empty lines and normalizing indentation.
 * 
 * This function is useful for formatting template strings or code snippets by:
 * 1. Removing empty lines at the start and end
 * 2. Detecting the minimum indentation level across all non-empty lines
 * 3. Removing that common indentation from each line
 * 
 * @param text - The text to format
 * @returns The formatted text with normalized indentation
 * 
 * @example
 * ```typescript
 * const code = prettyText(`
 *     function hello() {
 *         console.log("Hello");
 *     }
 * `);
 * // Returns:
 * // function hello() {
 * //     console.log("Hello");
 * // }
 * ```
 */
export function prettyText(text: string): string {
    const lines = text.split('\n');
    // Remove leading/trailing empty lines
    while (lines.length > 0 && lines[0].trim() === '') {
        lines.shift();
    }
    while (lines.length > 0 && lines[lines.length - 1].trim() === '') {
        lines.pop();
    }

    // Determine the minimum indentation
    let minIndent = Infinity;
    for (const line of lines) {
        const match = line.match(/^(\s*)\S/);
        if (match) {
            const indent = match[1].length;
            if (indent < minIndent) {
                minIndent = indent;
            }
        }
    }

    // Remove the minimum indentation from each line
    if (minIndent !== Infinity && minIndent > 0) {
        for (let i = 0; i < lines.length; i++) {
            if (lines[i].length >= minIndent) {
                lines[i] = lines[i].slice(minIndent);
            }
        }
    }

    return lines.join('\n');
}