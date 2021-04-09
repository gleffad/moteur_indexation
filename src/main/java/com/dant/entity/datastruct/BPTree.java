package com.dant.entity.datastruct;

import com.dant.webservices.Utils;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public abstract class BPTree implements IndexDataStructure {
    /**
     * We suppose that M is impair
     * * */
    public static int M = 3;

    /**
     * The id of the root node of the tree
     * */
    @Getter
    int rootId;
    /**
     * Total number of nodes in the tree
     * */
    @Getter
    int nbNodes;
    /**
     * If true, we don't have duplicates keys, so we overwrite the values associated to the key
     * true by default
     * */
    boolean allowDuplicates;
    //

    public BPTree(){
        this.rootId = 0;
        this.nbNodes = 0;
        this.allowDuplicates = true;
    }

    @Override public void insert(int key, int val) {
        Node root = getNodeAt(this.rootId);
        if(root == null){
            Node newNode = new Node(nbNodes);
            //nbNodes++;
            newNode.insertKey(key);
            newNode.childs[0] = saveValue(val);
            newNode.next = -1;
            saveNode(newNode);
            this.rootId = newNode.id;
        }else{
            Split split = insert(key, val, root);
            if(split!=null){
                Node newRoot = new Node(nbNodes);
                //nbNodes++;
                newRoot.next = -2;
                newRoot.insertKey(split.middle);
                newRoot.childs[0] = split.left;
                newRoot.childs[1] = split.right;
                this.rootId = newRoot.id;
                saveNode(newRoot);
            }
        }
    }
    private Split insert(int key, int val, Node root){
        if(root.next==-2){
            //We are internal
            int o;
            for (o=0; o < root.n; o++) {
                if(root.keys[o]>key){
                    break;
                }
            }
            Split res = insert(key, val, getNodeAt(root.childs[o]));
            if(res!=null){
                if(root.isFull()){
                    Node newNode = new Node(nbNodes);
                    //Vu qu'on est un noeud interne
                    newNode.next = -2;
                    //nbNodes++;
                    //Séquence ordonnée
                    int j=0;
                    int keyAdditional;
                    int valAdditional;
                    while(j<root.n){
                        if(root.keys[j]>res.middle){
                            break;
                        }
                        j++;
                    }
                    if(j<root.n){
                        keyAdditional = root.keys[root.n-1];
                        valAdditional = root.childs[root.n];
                        for(int k=root.n-1; k > j; k--){
                            root.keys[k] = root.keys[k-1];
                            root.childs[k+1] = root.childs[k];
                        }
                        root.keys[j] = key;
                        root.childs[j+1] = res.right;
                        //Pour rien
                        root.childs[j] = res.left;
                    }else{
                        keyAdditional = res.middle;
                        valAdditional = res.right;
                    }


                    //Split
                    int firstN = root.n;
                    int middle = root.keys[M/2+1];
                    newNode.childs[0]=root.childs[M/2+2];
                    j=1;
                    root.keys[M/2+1] = -1;
                    root.childs[M/2+2] = -1;
                    root.n--;
                    for (int i = M/2+2; i < firstN; i++) {
                        newNode.insertKey(root.keys[i]);
                        newNode.childs[j++] = root.childs[i+1];
                        root.keys[i] = -1;
                        root.childs[i+1] = -1;
                        root.n--;
                    }
                    newNode.insertKey(keyAdditional);
                    newNode.childs[newNode.n] = valAdditional;

                    //Sauvegarde du changement
                    saveNode(newNode);
                    saveNode(root);
                    return new Split(root.id, middle, newNode.id);

                }else{
                    int keyPos = root.insertKey(res.middle);
                    for (int j = root.n; j > keyPos+1; j--) {
                        root.childs[j] = root.childs[j-1];
                    }
                    root.childs[keyPos+1]=res.right;

                    //Inutile car identiques
                    root.childs[keyPos] = res.left;

                    saveNode(root);
                    return null;
                }
            }else{
                return null;
            }
        }else{
            //Here, we are in a leaf node
            if(!allowDuplicates){
                //We verify if the key is already in the leaf
                for (int i = 0; i < root.n; i++) {
                    if(root.keys[i]==key){
                        //We treat the duplicate key
                        treatDuplicateKey(i, val, root);
                        return null;
                    }
                }
            }
            if(root.isFull()){
                //Mise à jour des liens
                Node newNode = new Node(nbNodes);
                newNode.next = root.next;
                root.next = newNode.id;

                //Séquence ordonnée
                int j=0;
                int keyAdditional;
                int valAdditional;
                while(j<root.n){
                    if(root.keys[j]>key){
                        break;
                    }
                    j++;
                }
                if(j<root.n){
                    keyAdditional = root.keys[root.n-1];
                    valAdditional = root.childs[root.n-1];
                    for(int k=root.n-1; k > j; k--){
                        root.keys[k] = root.keys[k-1];
                        root.childs[k] = root.childs[k-1];
                    }
                    root.keys[j] = key;
                    saveInLeafAtPos(root, j, val);
                }else{
                    keyAdditional = key;
                    valAdditional = saveValue(val);
                }

                //Split
                j=0;
                int firstN = root.n;
                for (int i = M/2+1; i < firstN; i++) {
                    newNode.insertKey(root.keys[i]);
                    newNode.childs[j++] = root.childs[i];
                    root.childs[i] = -1;
                    root.keys[i] = -1;
                    root.n--;
                }
                newNode.insertKey(keyAdditional);
                newNode.childs[j]=valAdditional;

                //Sauvegarde du changement
                saveNode(newNode);
                saveNode(root);
                return new Split(root.id, newNode.keys[0], newNode.id);

            }else{
                int posKey = root.insertKey(key);
                for (int i = root.n-1; i > posKey ; i--) {
                    root.childs[i] = root.childs[i-1];
                }
                root.childs[posKey] = saveValue(val);
                saveNode(root);
                return null;
            }
        }
    }

    @Override public List<Integer> get(int key) {
        List<Integer> result = new LinkedList<>();
        Node leaf = getLeafNodeFromKey(getNodeAt(rootId), key);
        if(leaf != null){
            int pos = leaf.getFirstPositionOfKey(key);
            if(pos != -1){
                if(! allowDuplicates){
                    return getAllValuesInPos(leaf, pos);
                    //result.add(leaf.childs[pos]);
                }else{
                    boolean stillRemains = true;
                    while(stillRemains){
                        result.add(leaf.childs[pos]);
                        if(pos == leaf.n - 1 ){
                            if(leaf.next == -1){
                                stillRemains = false;
                                continue;
                            }else{
                                pos = 0;
                                leaf = getNodeAt(leaf.next);
                            }
                        }else{
                            pos++;
                        }if(leaf.keys[pos] != key){
                            stillRemains = false;
                        }
                    }
                }
            }
        }
        return result;
    }


    @Override public List<Integer> get(final int key, final String operator ){
        if(operator==null || operator.isEmpty() || !Utils.isWhereOperator(operator)){
            return get(key);
        }
        List<Integer> result = new LinkedList<>();
        Node curNode;
        boolean cont;
        switch (operator){
            case "=":
                return get(key);
            case "<":
                curNode = getLeftLeafNode();
                int to;
                cont = true;
                while(cont){
                    to=0;
                    for (; to < curNode.n; to++) {
                        if(curNode.childs[to]>=key){
                            to = to - 1;
                            break;
                        }
                    }
                    for (int i = 0; i < to; i++) {
                        result.addAll(getAllValuesInPos(curNode, i));
                    }
                    if(curNode.n <= to){
                        if(curNode.next == -1){
                            curNode = null;
                        }else{
                            curNode = getNodeAt(curNode.next);
                        }
                        if(curNode==null){
                            cont = false;
                        }
                    }else{
                        cont = false;
                    }
                }
                return result;
            case ">":
                curNode = getLeafNodeFromKey(key);
                int i = curNode.getPositionWhereShouldPutKey(key);
                for(;i<curNode.n;i++){
                    result.addAll(getAllValuesInPos(curNode, i));
                }
                if(curNode.next == -1){
                    cont = false;
                }else{
                    curNode = getNodeAt(curNode.next);
                    cont = true;
                }
                while(cont){
                    to = curNode.getPositionWhereShouldPutKey(key);
                    for (int j = 0; j < curNode.n; j++) {
                        result.addAll(getAllValuesInPos(curNode, j));
                    }
                    if(curNode.next == -1){
                        curNode = null;
                    }else{
                        curNode = getNodeAt(curNode.next);
                    }

                    if(curNode==null){
                        cont = false;
                    }
                }
                return result;
            default:
                return get(key);
        }
    }


    public abstract Node getNodeAt(int position);
    public abstract int appendNode(Node node);
    public abstract void saveNode(Node node, int position);
    public abstract void treatDuplicateKey(final int position, final int value, final Node leaf);
    public abstract List<Integer> getAllValuesInPos(final Node leaf, final int pos);
    public abstract int saveValue(int value);
    public  void saveNode(Node node){
        saveNode(node, node.id);
    }
    public void saveInLeafAtPos(final Node leaf, int pos, int val){
        leaf.childs[pos] = saveValue(val);
    }


    /**
     * @return Returns the leaf node where we should find a given key
     * */
    public Node getLeafNodeFromKey(int key){
        return getLeafNodeFromKey(getNodeAt(rootId), key);
    }
    public Node getLeafNodeFromKey(Node root, int key){
        if(root.next == -2){
            //We are in an internal node
            return getLeafNodeFromKey(getNodeAt(root.childs[root.getPositionWhereShouldPutKey(key)]), key);
        }else{
            //We are in a leaf so there are no children, so we return ourself because we cannot go deeper
            return root;
        }
    }


    /**
     * @return Returns the position of leaf node where we should find a given key
     * */
    @Deprecated public int getLeafNodePositionFromKey(int key){
        return getLeafNodePositionFromKey(rootId, key);
    }
    @Deprecated public int getLeafNodePositionFromKey(int rootPos, int key){
        Node root = getNodeAt(rootPos);
        if(root.next == -2){
            //We are in an internal node
            return getLeafNodePositionFromKey(root.childs[root.getPositionWhereShouldPutKey(key)], key);
        }else{
            //We are in a leaf so there are no children, so we return ourself because we cannot go deeper
            return rootPos;
        }
    }


    /**@return Returns the most left node*/
    public Node getLeftLeafNode(){
        return getLeftLeafNode(getNodeAt(rootId));
    }
    public Node getLeftLeafNode(final Node root){
        if(root==null)return null;
        if(root.next == -2){
            return getLeftLeafNode(getNodeAt(root.childs[0]));
        }else{
            return root;
        }
    }

    @Override public int hashCode() {
        return Objects.hash(nbNodes);
    }
}
