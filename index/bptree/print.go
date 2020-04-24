package bptree

import (
	"fmt"
	"strings"
)

// Print traverses the entire tree and pretty prints it. This should be
// used for debugging only.
func Print(tree *BPlusTree) error {
	if len(tree.root.entries) == 0 {
		fmt.Println("(empty)")
		return nil
	}

	return printRec(tree, tree.root, 0)
}

func printRec(tree *BPlusTree, n *node, indent int) error {
	if indent > 0 {
		fmt.Print("|" + strings.Repeat("-", indent))
	}

	fmt.Printf("+ %s\n", n)

	for i := 0; i < len(n.children); i++ {
		child, err := tree.fetch(n.children[i])
		if err != nil {
			return err
		}

		if err := printRec(tree, child, indent+2); err != nil {
			return nil
		}
	}

	return nil
}
