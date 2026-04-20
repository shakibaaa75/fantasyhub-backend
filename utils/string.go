package utils

// IntersectStrings returns the intersection of two string slices.
// Time complexity: O(n + m) where n = len(a), m = len(b).
func IntersectStrings(a, b []string) []string {
	set := make(map[string]bool, len(a))
	for _, item := range a {
		set[item] = true
	}

	var result []string
	for _, item := range b {
		if set[item] {
			result = append(result, item)
		}
	}
	return result
}

// UnionStrings returns the union of two string slices (deduplicated).
// Time complexity: O(n + m).
func UnionStrings(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	var result []string
	for _, item := range a {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	for _, item := range b {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// JaccardSimilarity computes the Jaccard similarity coefficient between two string sets.
// Jaccard(A, B) = |A ∩ B| / |A ∪ B|
// Returns 0.0 if both sets are empty, 1.0 if both sets are identical and non-empty.
func JaccardSimilarity(a, b []string) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 0.0
	}

	intersection := IntersectStrings(a, b)
	union := UnionStrings(a, b)

	if len(union) == 0 {
		return 0.0
	}

	return float64(len(intersection)) / float64(len(union))
}

// SharedTagCount returns the number of tags shared between two string slices.
func SharedTagCount(a, b []string) int {
	return len(IntersectStrings(a, b))
}

// IsMatchCompatible checks whether two tag sets are compatible for matching
// based on a minimum Jaccard similarity threshold and minimum shared tag count.
func IsMatchCompatible(userTags, candidateTags []string, minSimilarity float64, minSharedTags int) (bool, float64, []string) {
	shared := IntersectStrings(userTags, candidateTags)
	sharedCount := len(shared)

	// Quick rejection: not enough shared tags
	if sharedCount < minSharedTags {
		return false, 0, nil
	}

	// Compute Jaccard similarity
	similarity := JaccardSimilarity(userTags, candidateTags)

	// Check threshold
	if similarity >= minSimilarity {
		return true, similarity, shared
	}

	return false, similarity, nil
}

// StringSliceContains checks whether a string slice contains a specific string.
func StringSliceContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// DeduplicateStrings removes duplicate entries from a string slice, preserving order.
func DeduplicateStrings(slice []string) []string {
	seen := make(map[string]bool, len(slice))
	var result []string
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}
