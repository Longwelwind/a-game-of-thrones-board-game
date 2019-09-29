export default abstract class OrderType {
    id: string;
    name: string;
    starred: boolean;

    constructor(id: string, name: string, starred: boolean) {
        this.id = id;
        this.starred = starred;
    }

    abstract toString(): string;
}
