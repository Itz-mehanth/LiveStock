import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError, retry, tap } from 'rxjs/operators';

interface StockData {
  window_end: string;
  avg_price: number;
  symbol?: string;
  company?: string;
}

@Injectable({ providedIn: 'root' })
export class DataService {
  private apiUrl = 'http://localhost:5000/api';

  constructor(private http: HttpClient) {}

  getProcessedData(): Observable<StockData[]> {
    const url = `${this.apiUrl}/stock_data`;
    console.log(`[DEBUG] DataService: Fetching processed data from ${url}`);
    
    return this.http.get<StockData[]>(url).pipe(
      // --- THIS IS THE CHANGE ---
      // We are now logging the entire data array to the console, not just the count.
      tap(data => {
        console.log(`[DEBUG] DataService: Received ${data.length} records.`);
        console.log('[DEBUG] Data payload:', data); // This line prints the full result.
      }),
      retry(2),
      catchError(this.handleError)
    );
  }

  getFutureTrends(symbol: string, horizon: string): Observable<any> {
    const url = `${this.apiUrl}/future_trends`;
    const payload = { symbol, horizon };
    console.log(`[DEBUG] DataService: Requesting future trends from ${url} with payload:`, payload);

    return this.http.post<any>(url, payload).pipe(
      tap(response => console.log('[DEBUG] DataService: Received prediction response:', response)),
      retry(2),
      catchError(this.handleError)
    );
  }

  private handleError(error: HttpErrorResponse): Observable<never> {
    let errorMessage = 'An unknown error occurred';
    
    if (error.error instanceof ErrorEvent) {
      // A client-side or network error occurred.
      errorMessage = `Client-side error: ${error.error.message}`;
    } else {
      // The backend returned an unsuccessful response code.
      errorMessage = `Server returned code ${error.status}, error message is: ${error.message}`;
    }

    console.error(`[DEBUG] DataService Error: ${errorMessage}`, error);
    
    return throwError(() => new Error('Something bad happened; please try again later. Full error logged in console.'));
  }
}

