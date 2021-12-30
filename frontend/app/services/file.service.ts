import { Injectable } from "@angular/core";


@Injectable()
export class FileService {
	constructor() {}

    readJson(file, callbackload, callbackerror) {
        if (typeof (FileReader) !== 'undefined') {
            const reader = new FileReader();
      
            // handlers
            reader.onload = (e: any) => {
              const result = e.target.result;
              callbackload(JSON.parse(result))
              };
            reader.onerror = (error: any) => {
                if (callbackerror) callbackerror(error)
            }
      
            // Calls onload handler
            reader.readAsText(file);
            }
    }
}