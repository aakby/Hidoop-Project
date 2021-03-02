package ordo;

import java.util.Comparator;

public class SortComparatorImpl implements SortComparator {

	// Comparer cha�nes Selon l'ordre lexicographiqueo
	// Le r�sultat un entier repr�sentant la position relative des deux cha�nes 
	// positve si k1 est avant k2.
    // negative si k2 est avant k1.
	public int compare(String k1, String k2) {
		Comparator<String> comparator = String.CASE_INSENSITIVE_ORDER;
		int comparaison = comparator.compare(k1, k2);
		return comparaison;
	}

}
