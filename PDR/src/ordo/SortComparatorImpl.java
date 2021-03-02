package ordo;

import java.util.Comparator;

public class SortComparatorImpl implements SortComparator {

	// Comparer chaînes Selon l'ordre lexicographiqueo
	// Le résultat un entier représentant la position relative des deux chaînes 
	// positve si k1 est avant k2.
    // negative si k2 est avant k1.
	public int compare(String k1, String k2) {
		Comparator<String> comparator = String.CASE_INSENSITIVE_ORDER;
		int comparaison = comparator.compare(k1, k2);
		return comparaison;
	}

}
